// Package rabbitmqqueue implements the Hypercube queue contract using
// RabbitMQ quorum queues.
package rabbitmqqueue

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-hypercube/go-hypercube/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DefaultNamePrefix      = "hypercube:queue"
	DefaultPollingTimeout  = time.Second
	DefaultPollingInterval = 25 * time.Millisecond
)

// Option configures a Queue at construction time.
type Option func(*Queue)

// WithNamePrefix sets the isolation namespace used to derive physical RabbitMQ
// resource names. Instances in the same virtual host with the same prefix share
// queues, deliveries, dead letters, delay queues, and ID reservations.
func WithNamePrefix(prefix string) Option {
	return func(q *Queue) {
		q.namePrefix = prefix
	}
}

// WithPollingTimeout sets the maximum empty Pop wait. Non-positive values
// check once and return queue.ErrEmpty when no message is immediately ready.
func WithPollingTimeout(timeout time.Duration) Option {
	return func(q *Queue) {
		q.pollingTimeout = timeout
	}
}

// WithPollingInterval sets the interval between empty Basic.Get calls.
// Non-positive values are ignored.
func WithPollingInterval(interval time.Duration) Option {
	return func(q *Queue) {
		if interval > 0 {
			q.pollingInterval = interval
		}
	}
}

// Queue is safe for concurrent use. The caller owns the AMQP connection and
// must close it after closing this queue and stopping all workers.
type Queue struct {
	connection      *amqp.Connection
	namePrefix      string
	namespace       string
	pollingTimeout  time.Duration
	pollingInterval time.Duration

	mu      sync.Mutex
	closed  bool
	done    chan struct{}
	batches map[*deliveryBatch]struct{}
}

// RabbitMQQueue is an alias for callers that prefer an explicit driver name.
type RabbitMQQueue = Queue

var (
	_ queue.Queue           = (*Queue)(nil)
	_ queue.RedriveProvider = (*Queue)(nil)
)

// New wraps an AMQP connection without contacting RabbitMQ. The caller retains
// ownership of the connection. A nil or closed connection fails lazily when an
// operation first needs a channel.
func New(connection *amqp.Connection, options ...Option) *Queue {
	q := &Queue{
		connection:      connection,
		namePrefix:      DefaultNamePrefix,
		pollingTimeout:  DefaultPollingTimeout,
		pollingInterval: DefaultPollingInterval,
		done:            make(chan struct{}),
		batches:         make(map[*deliveryBatch]struct{}),
	}
	for _, option := range options {
		if option != nil {
			option(q)
		}
	}
	q.namespace = physicalNamespace(q.namePrefix)
	return q
}

// Close invalidates and closes every outstanding delivery channel, causing
// RabbitMQ to requeue its unacknowledged deliveries. It does not close the
// caller-owned AMQP connection. Close is idempotent.
func (q *Queue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	close(q.done)
	batches := make([]*deliveryBatch, 0, len(q.batches))
	for batch := range q.batches {
		batches = append(batches, batch)
	}
	q.mu.Unlock()

	var closeErrors []error
	for _, batch := range batches {
		if err := batch.invalidate(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeErrors = append(closeErrors, err)
		}
	}
	return errors.Join(closeErrors...)
}

// Push snapshots and publishes each message independently. RabbitMQ-backed ID
// reservations are permanent: an ID cannot be reused in the same vhost/prefix.
func (q *Queue) Push(ctx context.Context, msgs ...*queue.Message) error {
	return q.batch(ctx, msgs, q.pushOne)
}

func (q *Queue) pushOne(ctx context.Context, msg *queue.Message) error {
	if msg.ID == "" {
		msg.ID = rand.Text()
	}

	milliseconds, err := delayMilliseconds(msg.Delay)
	if err != nil {
		return err
	}
	body, err := encodeEnvelope(envelopeFromMessage(msg, 0))
	if err != nil {
		return err
	}

	ch, err := q.openChannel(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("rabbitmq queue: enable publisher confirms: %w", err)
	}

	if err := q.declareMarkerQueue(ch, msg.ID); err != nil {
		return err
	}
	markerName := q.markerQueueName(msg.ID)
	if err := publishReservationConfirmed(ctx, ch, markerName, msg.ID); err != nil {
		if errors.Is(err, ErrPublishRejected) {
			state, inspectErr := ch.QueueInspect(markerName)
			if inspectErr == nil && state.Messages > 0 {
				return queue.ErrAlreadyExists
			}
			if inspectErr != nil {
				return errors.Join(err, fmt.Errorf("rabbitmq queue: inspect ID reservation: %w", inspectErr))
			}
		}
		return err
	}

	// Reserve the ID before provisioning or publishing its target. This gives a
	// retained duplicate precedence over target-specific failures and prevents
	// concurrent producers from both publishing the same logical ID.
	routingKey := q.workQueueName(msg.QueueName)
	if milliseconds == 0 {
		if err := q.declareWorkQueue(ch, msg.QueueName); err != nil {
			return err
		}
	} else {
		routingKey, err = q.declareDelayQueue(ch, msg.QueueName, milliseconds)
		if err != nil {
			return err
		}
	}

	// The reservation intentionally remains if provisioning or publication has
	// an unknown or failed outcome. Removing it could allow a new logical message
	// to collide with a stale physical copy created by an at-least-once transition.
	if err := publishMessageConfirmed(ctx, ch, routingKey, msg.ID, body); err != nil {
		return err
	}
	return nil
}

func (q *Queue) batch(
	ctx context.Context,
	msgs []*queue.Message,
	apply func(context.Context, *queue.Message) error,
) error {
	if len(msgs) == 0 {
		return nil
	}
	var failed []queue.ItemError
	for index, msg := range msgs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if msg == nil {
			failed = append(failed, queue.ItemError{Index: index, Msg: nil, Err: queue.ErrInvalidArgument})
			continue
		}
		err := apply(ctx, msg)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if err != nil {
			failed = append(failed, queue.ItemError{Index: index, Msg: msg, Err: err})
		}
	}
	if len(failed) > 0 {
		return &queue.BatchError{Failed: failed}
	}
	return nil
}

func (q *Queue) openChannel(ctx context.Context) (*amqp.Channel, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q.mu.Lock()
	closed := q.closed
	connection := q.connection
	q.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	if connection == nil {
		return nil, fmt.Errorf("rabbitmq queue: nil AMQP connection: %w", queue.ErrInvalidArgument)
	}
	if connection.IsClosed() {
		return nil, amqp.ErrClosed
	}
	ch, err := connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq queue: open channel: %w", err)
	}
	if err := ctx.Err(); err != nil {
		_ = ch.Close()
		return nil, err
	}
	q.mu.Lock()
	closed = q.closed
	q.mu.Unlock()
	if closed {
		_ = ch.Close()
		return nil, ErrClosed
	}
	return ch, nil
}

func (q *Queue) track(batch *deliveryBatch) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	q.batches[batch] = struct{}{}
	return true
}

func (q *Queue) untrack(batch *deliveryBatch) {
	q.mu.Lock()
	delete(q.batches, batch)
	q.mu.Unlock()
}

func (q *Queue) isClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}
