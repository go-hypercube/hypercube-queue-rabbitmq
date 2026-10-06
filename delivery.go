package rabbitmqqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-hypercube/go-hypercube/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

type settlement uint8

const (
	settlementAck settlement = iota
	settlementRetry
	settlementDead
)

type deliveryToken struct {
	owner *Queue
	batch *deliveryBatch
	tag   uint64
}

type deliveryRecord struct {
	token   *deliveryToken
	message queue.Message
}

type deliveryBatch struct {
	owner   *Queue
	channel *amqp.Channel
	lock    chan struct{}
	records map[uint64]*deliveryRecord
	closed  bool
}

type claimedDelivery struct {
	delivery amqp.Delivery
	message  *queue.Message
}

// Pop polls RabbitMQ until it claims at least one delivery, the context ends,
// or the configured polling timeout expires. Each successful call retains its
// channel until all returned deliveries settle.
func (q *Queue) Pop(ctx context.Context, queueName string, maxMessages int) ([]*queue.Message, error) {
	if maxMessages <= 0 {
		return nil, queue.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q.isClosed() {
		return nil, ErrClosed
	}
	deadline := time.Now().Add(q.pollingTimeout)

	ch, err := q.openChannel(ctx)
	if err != nil {
		return nil, err
	}
	closeEvents := ch.NotifyClose(make(chan *amqp.Error, 1))
	closeOnReturn := true
	defer func() {
		if closeOnReturn {
			_ = ch.Close()
		}
	}()
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("rabbitmq queue: enable delivery-channel confirms: %w", err)
	}
	if err := q.declareWorkQueue(ch, queueName); err != nil {
		return nil, err
	}

	claimed := make([]claimedDelivery, 0, min(maxMessages, 16))
	for len(claimed) < maxMessages {
		if len(claimed) == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if q.isClosed() {
				return nil, ErrClosed
			}
		} else if ctx.Err() != nil || q.isClosed() {
			break
		}

		delivery, ok, err := ch.Get(q.workQueueName(queueName), false)
		if err != nil {
			return nil, fmt.Errorf("rabbitmq queue: get from %q: %w", queueName, err)
		}
		if ok {
			msg, err := deliveryMessage(delivery.Body, delivery.Headers)
			if err != nil {
				return nil, err
			}
			if msg.QueueName != queueName {
				return nil, fmt.Errorf("rabbitmq queue: message %q belongs to queue %q, not %q", msg.ID, msg.QueueName, queueName)
			}
			claimed = append(claimed, claimedDelivery{delivery: delivery, message: msg})
			continue
		}
		if len(claimed) > 0 {
			break
		}

		wait := min(q.pollingInterval, time.Until(deadline))
		if wait <= 0 {
			return nil, queue.ErrEmpty
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return nil, ctx.Err()
		case <-q.done:
			stopTimer(timer)
			return nil, ErrClosed
		case channelErr, ok := <-closeEvents:
			stopTimer(timer)
			if ok && channelErr != nil {
				return nil, fmt.Errorf("rabbitmq queue: delivery channel closed: %v", channelErr)
			}
			return nil, amqp.ErrClosed
		case <-timer.C:
		}
	}

	batch := &deliveryBatch{
		owner:   q,
		channel: ch,
		lock:    make(chan struct{}, 1),
		records: make(map[uint64]*deliveryRecord, len(claimed)),
	}
	batch.lock <- struct{}{}
	messages := make([]*queue.Message, 0, len(claimed))
	for _, item := range claimed {
		token := &deliveryToken{owner: q, batch: batch, tag: item.delivery.DeliveryTag}
		item.message.DriverData = token
		batch.records[token.tag] = &deliveryRecord{
			token:   token,
			message: cloneMessage(item.message),
		}
		messages = append(messages, item.message)
	}
	if ch.IsClosed() {
		return nil, amqp.ErrClosed
	}
	if !q.track(batch) {
		return nil, ErrClosed
	}
	closeOnReturn = false
	go batch.watch(closeEvents)
	return messages, nil
}

// Ack permanently removes current RabbitMQ deliveries.
func (q *Queue) Ack(ctx context.Context, msgs ...*queue.Message) error {
	return q.batch(ctx, msgs, func(ctx context.Context, msg *queue.Message) error {
		return q.settleOne(ctx, msg, settlementAck)
	})
}

// Retry publishes each updated delivery to its active or delay queue and only
// then acknowledges the immutable original.
func (q *Queue) Retry(ctx context.Context, msgs ...*queue.Message) error {
	return q.batch(ctx, msgs, func(ctx context.Context, msg *queue.Message) error {
		return q.settleOne(ctx, msg, settlementRetry)
	})
}

// DeadLetter publishes each updated delivery to its explicit dead queue and
// only then acknowledges the immutable original.
func (q *Queue) DeadLetter(ctx context.Context, msgs ...*queue.Message) error {
	return q.batch(ctx, msgs, func(ctx context.Context, msg *queue.Message) error {
		return q.settleOne(ctx, msg, settlementDead)
	})
}

func (q *Queue) settleOne(ctx context.Context, msg *queue.Message, operation settlement) error {
	token, ok := msg.DriverData.(*deliveryToken)
	if !ok || token == nil || token.owner != q || token.batch == nil {
		return queue.ErrNotFound
	}
	if err := token.batch.acquire(ctx); err != nil {
		return err
	}
	defer token.batch.release()

	batch := token.batch
	record := batch.records[token.tag]
	if batch.closed || batch.channel.IsClosed() || record == nil || record.token != token {
		if batch.channel.IsClosed() {
			batch.markClosedLocked()
		}
		return queue.ErrNotFound
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if operation != settlementAck {
		updated := record.message
		updated.Payload = cloneBytes(msg.Payload)
		updated.Extra = cloneBytes(msg.Extra)
		updated.Delay = msg.Delay
		updated.DriverData = nil
		body, err := encodeEnvelope(envelopeFromMessage(&updated, record.message.Attempt))
		if err != nil {
			return err
		}

		routingKey := q.workQueueName(record.message.QueueName)
		switch operation {
		case settlementRetry:
			milliseconds, err := delayMilliseconds(msg.Delay)
			if err != nil {
				return err
			}
			if milliseconds > 0 {
				routingKey, err = q.declareDelayQueue(batch.channel, record.message.QueueName, milliseconds)
			} else {
				err = q.declareWorkQueue(batch.channel, record.message.QueueName)
			}
			if err != nil {
				batch.invalidateLocked()
				return err
			}
		case settlementDead:
			if err := q.declareDeadQueue(batch.channel, record.message.QueueName); err != nil {
				batch.invalidateLocked()
				return err
			}
			routingKey = q.deadQueueName(record.message.QueueName)
		}

		if err := publishMessageConfirmed(ctx, batch.channel, routingKey, record.message.ID, body); err != nil {
			if !errors.Is(err, ErrPublishRejected) {
				batch.invalidateLocked()
			}
			return err
		}
	}

	if err := batch.channel.Ack(token.tag, false); err != nil {
		batch.invalidateLocked()
		return fmt.Errorf("rabbitmq queue: acknowledge message %q: %w", record.message.ID, err)
	}
	// QueueInspect is a synchronous method on the same channel. Its response
	// proves that the preceding asynchronous basic.ack was processed.
	if _, err := batch.channel.QueueInspect(q.workQueueName(record.message.QueueName)); err != nil {
		batch.invalidateLocked()
		return fmt.Errorf("rabbitmq queue: confirm acknowledgement of message %q: %w", record.message.ID, err)
	}

	delete(batch.records, token.tag)
	if len(batch.records) == 0 {
		batch.closed = true
		q.untrack(batch)
		_ = batch.channel.Close()
	}
	return nil
}

func (batch *deliveryBatch) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-batch.lock:
		return nil
	}
}

func (batch *deliveryBatch) acquireUninterruptibly() {
	<-batch.lock
}

func (batch *deliveryBatch) release() {
	batch.lock <- struct{}{}
}

func (batch *deliveryBatch) watch(closeEvents <-chan *amqp.Error) {
	<-closeEvents
	batch.acquireUninterruptibly()
	batch.markClosedLocked()
	batch.release()
}

func (batch *deliveryBatch) markClosedLocked() {
	if batch.closed {
		return
	}
	batch.closed = true
	clear(batch.records)
	batch.owner.untrack(batch)
}

func (batch *deliveryBatch) invalidateLocked() {
	if !batch.closed {
		batch.closed = true
		clear(batch.records)
		batch.owner.untrack(batch)
	}
	_ = batch.channel.Close()
}

func (batch *deliveryBatch) invalidate() error {
	batch.acquireUninterruptibly()
	defer batch.release()
	if batch.closed {
		return nil
	}
	batch.closed = true
	clear(batch.records)
	batch.owner.untrack(batch)
	return batch.channel.Close()
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
