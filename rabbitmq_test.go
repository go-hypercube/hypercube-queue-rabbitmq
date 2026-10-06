package rabbitmqqueue

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-hypercube/go-hypercube/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	_ queue.Queue           = (*Queue)(nil)
	_ queue.RedriveProvider = (*Queue)(nil)
)

func TestNewAppliesDefaultsAndOptions(t *testing.T) {
	q := New(nil)
	if q.namePrefix != DefaultNamePrefix {
		t.Fatalf("default prefix = %q, want %q", q.namePrefix, DefaultNamePrefix)
	}
	if q.pollingTimeout != DefaultPollingTimeout {
		t.Fatalf("default polling timeout = %s", q.pollingTimeout)
	}
	if q.pollingInterval != DefaultPollingInterval {
		t.Fatalf("default polling interval = %s", q.pollingInterval)
	}

	configured := New(nil,
		WithNamePrefix("tenant-a"),
		WithPollingTimeout(3*time.Second),
		WithPollingInterval(125*time.Millisecond),
		WithPollingInterval(0),
	)
	if configured.namePrefix != "tenant-a" {
		t.Fatalf("configured prefix = %q", configured.namePrefix)
	}
	if configured.pollingTimeout != 3*time.Second {
		t.Fatalf("configured polling timeout = %s", configured.pollingTimeout)
	}
	if configured.pollingInterval != 125*time.Millisecond {
		t.Fatalf("configured polling interval = %s", configured.pollingInterval)
	}
	if configured.namespace == q.namespace {
		t.Fatal("different prefixes must use different physical namespaces")
	}
}

func TestPhysicalNamesAreDeterministicBoundedAndIsolated(t *testing.T) {
	q := New(nil, WithNamePrefix(strings.Repeat("prefix", 1000)))
	peer := New(nil, WithNamePrefix(q.namePrefix))
	other := New(nil, WithNamePrefix(q.namePrefix+"-other"))
	logical := strings.Repeat("queue\x00雪", 1000)
	id := strings.Repeat("id\x00雪", 1000)

	names := []string{
		q.workQueueName(logical),
		q.deadQueueName(logical),
		q.delayQueueName(logical, 1234),
		q.markerQueueName(id),
	}
	for _, name := range names {
		if len(name) > 255 {
			t.Fatalf("physical name has %d bytes: %q", len(name), name)
		}
	}
	if q.workQueueName(logical) != peer.workQueueName(logical) {
		t.Fatal("same prefix and logical name must map to the same queue")
	}
	if q.workQueueName(logical) == other.workQueueName(logical) {
		t.Fatal("different prefixes must isolate physical queues")
	}
	if q.workQueueName(logical) == q.deadQueueName(logical) {
		t.Fatal("work and dead queues must differ")
	}
}

func TestDelayMillisecondsRoundsUp(t *testing.T) {
	tests := []struct {
		delay time.Duration
		want  int64
	}{
		{delay: -time.Second, want: 0},
		{delay: 0, want: 0},
		{delay: time.Nanosecond, want: 1},
		{delay: time.Millisecond, want: 1},
		{delay: time.Millisecond + time.Nanosecond, want: 2},
		{delay: time.Duration(1<<63 - 1), want: int64((time.Duration(1<<63-1) / time.Millisecond) + 1)},
	}
	for _, test := range tests {
		got, err := delayMilliseconds(test.delay)
		if err != nil {
			t.Fatalf("delayMilliseconds(%s): %v", test.delay, err)
		}
		if got != test.want {
			t.Fatalf("delayMilliseconds(%s) = %d, want %d", test.delay, got, test.want)
		}
	}

}

func TestEnvelopeSnapshotsOpaqueFieldsAndDeliveryCounters(t *testing.T) {
	shared := []byte{0, 255, 128, 'a'}
	input := &queue.Message{
		ID:                "id\x00\xff",
		QueueName:         "queue\x00\xff",
		Namespace:         "namespace\x00\xff",
		JobName:           "job\x00\xff",
		Delay:             17*time.Millisecond + time.Nanosecond,
		Payload:           shared,
		Extra:             shared,
		Attempt:           99,
		VisibilityTimeout: time.Minute + 7*time.Nanosecond,
		DriverData:        make(chan struct{}),
	}
	body, err := encodeEnvelope(envelopeFromMessage(input, 0))
	if err != nil {
		t.Fatal(err)
	}
	shared[0] = 'X'
	input.ID = "changed"
	input.QueueName = "changed"

	first, err := deliveryMessage(body, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != "id\x00\xff" || first.QueueName != "queue\x00\xff" {
		t.Fatalf("opaque string fields changed: %#v", first)
	}
	if first.Attempt != 1 || first.Delay != 0 || first.DriverData != nil {
		t.Fatalf("first delivery state = attempt %d, delay %s, data %#v", first.Attempt, first.Delay, first.DriverData)
	}
	if first.Payload[0] != 0 || first.Extra[0] != 0 {
		t.Fatalf("snapshot aliased caller bytes: payload=%v extra=%v", first.Payload, first.Extra)
	}
	first.Payload[0] = 'P'
	if first.Extra[0] != 0 {
		t.Fatal("payload and extra must not alias")
	}
	if first.VisibilityTimeout != time.Minute+7*time.Nanosecond {
		t.Fatalf("visibility timeout = %s", first.VisibilityTimeout)
	}

	redelivered, err := deliveryMessage(body, amqp.Table{"x-delivery-count": int64(2)})
	if err != nil {
		t.Fatal(err)
	}
	if redelivered.Attempt != 3 {
		t.Fatalf("delivery-count attempt = %d, want 3", redelivered.Attempt)
	}
	acquired, err := deliveryMessage(body, amqp.Table{
		"x-delivery-count": int64(99),
		"x-acquired-count": int64(4),
	})
	if err != nil {
		t.Fatal(err)
	}
	if acquired.Attempt != 4 {
		t.Fatalf("acquired-count attempt = %d, want 4", acquired.Attempt)
	}
}

func TestExplicitRetryAttemptBecomesNextDelivery(t *testing.T) {
	msg := &queue.Message{ID: "id", QueueName: "jobs", Attempt: 4}
	body, err := encodeEnvelope(envelopeFromMessage(msg, msg.Attempt))
	if err != nil {
		t.Fatal(err)
	}
	next, err := deliveryMessage(body, nil)
	if err != nil {
		t.Fatal(err)
	}
	if next.Attempt != 5 {
		t.Fatalf("next attempt = %d, want 5", next.Attempt)
	}
}

func TestValidationAndEmptyBatchDoNotNeedConnection(t *testing.T) {
	q := New(nil)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := q.Push(canceled); err != nil {
		t.Fatalf("empty Push = %v, want nil", err)
	}
	if err := q.Ack(canceled); err != nil {
		t.Fatalf("empty Ack = %v, want nil", err)
	}
	if err := q.Retry(canceled); err != nil {
		t.Fatalf("empty Retry = %v, want nil", err)
	}
	if err := q.DeadLetter(canceled); err != nil {
		t.Fatalf("empty DeadLetter = %v, want nil", err)
	}
	if _, err := q.Pop(context.Background(), "jobs", 0); !errors.Is(err, queue.ErrInvalidArgument) {
		t.Fatalf("Pop error = %v, want ErrInvalidArgument", err)
	}
	if _, err := q.Redrive(context.Background(), "jobs", 0); !errors.Is(err, queue.ErrInvalidArgument) {
		t.Fatalf("Redrive error = %v, want ErrInvalidArgument", err)
	}

	inputs := []*queue.Message{nil, nil}
	err := q.Push(context.Background(), inputs...)
	var batch *queue.BatchError
	if !errors.As(err, &batch) || len(batch.Failed) != 2 {
		t.Fatalf("nil-only batch error = %#v", err)
	}
	for index, failure := range batch.Failed {
		if failure.Index != index || failure.Msg != nil || !errors.Is(failure.Err, queue.ErrInvalidArgument) {
			t.Fatalf("failure %d = %#v", index, failure)
		}
	}
}

func TestNilConnectionAndCloseErrors(t *testing.T) {
	q := New(nil)
	msg := &queue.Message{QueueName: "jobs"}
	err := q.Push(context.Background(), msg)
	var batch *queue.BatchError
	if !errors.As(err, &batch) || len(batch.Failed) != 1 {
		t.Fatalf("Push error = %#v, want one-item BatchError", err)
	}
	if !errors.Is(batch.Failed[0].Err, queue.ErrInvalidArgument) {
		t.Fatalf("Push item error = %v, want ErrInvalidArgument", batch.Failed[0].Err)
	}
	if msg.ID == "" {
		t.Fatal("Push must assign an ID before backend work")
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	if _, err := q.Pop(context.Background(), "jobs", 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Pop after Close = %v, want ErrClosed", err)
	}
}
