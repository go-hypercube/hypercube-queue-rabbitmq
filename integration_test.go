package rabbitmqqueue

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/go-hypercube/go-hypercube/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRabbitMQQueueLifecycle(t *testing.T) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		t.Skip("RABBITMQ_URL is not set")
	}
	connection, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial RabbitMQ: %v", err)
	}
	prefix := "hypercube-test:" + rand.Text()
	q := New(connection,
		WithNamePrefix(prefix),
		WithPollingTimeout(3*time.Second),
		WithPollingInterval(10*time.Millisecond),
	)
	const queueName = "jobs\x00雪"
	const id = "message\x00雪"
	const retryDelay = 100 * time.Millisecond
	t.Cleanup(func() {
		_ = q.Close()
		ch, channelErr := connection.Channel()
		if channelErr == nil {
			_, _ = ch.QueueDelete(q.workQueueName(queueName), false, false, false)
			_, _ = ch.QueueDelete(q.deadQueueName(queueName), false, false, false)
			milliseconds, _ := delayMilliseconds(retryDelay)
			_, _ = ch.QueueDelete(q.delayQueueName(queueName, milliseconds), false, false, false)
			_, _ = ch.QueueDelete(q.markerQueueName(id), false, false, false)
			_ = ch.Close()
		}
		_ = connection.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	input := &queue.Message{
		ID: id, QueueName: queueName, Namespace: "app", JobName: "work",
		Payload: []byte{0, 255, 'a'}, Extra: []byte("before"), Attempt: 99,
		VisibilityTimeout: time.Minute + 7*time.Nanosecond,
	}
	if err := q.Push(ctx, input); err != nil {
		t.Fatalf("Push: %v", err)
	}
	duplicate := &queue.Message{ID: id, QueueName: "other"}
	assertBatchItemError(t, q.Push(ctx, duplicate), queue.ErrAlreadyExists)

	input.Payload[0] = 'X'
	first := popOneIntegration(t, ctx, q, queueName)
	if first.Attempt != 1 || first.Delay != 0 {
		t.Fatalf("first delivery attempt=%d delay=%s", first.Attempt, first.Delay)
	}
	if first.Payload[0] != 0 || string(first.Extra) != "before" {
		t.Fatalf("first delivery did not preserve snapshot: %#v", first)
	}
	first.Payload = []byte("retry payload")
	first.Extra = []byte("retry extra")
	first.Delay = retryDelay
	if err := q.Retry(ctx, first); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	second := popOneIntegration(t, ctx, q, queueName)
	if second.Attempt != 2 || second.Delay != 0 {
		t.Fatalf("second delivery attempt=%d delay=%s", second.Attempt, second.Delay)
	}
	if string(second.Payload) != "retry payload" || string(second.Extra) != "retry extra" {
		t.Fatalf("retry state was not preserved: %#v", second)
	}
	second.Payload = []byte("dead payload")
	second.Extra = []byte("dead extra")
	if err := q.DeadLetter(ctx, second); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}

	n, err := q.Redrive(ctx, queueName, 1)
	if err != nil || n != 1 {
		t.Fatalf("Redrive = (%d, %v), want (1, nil)", n, err)
	}
	redriven := popOneIntegration(t, ctx, q, queueName)
	if redriven.Attempt != 1 || redriven.Delay != 0 {
		t.Fatalf("redriven delivery attempt=%d delay=%s", redriven.Attempt, redriven.Delay)
	}
	if string(redriven.Payload) != "dead payload" || string(redriven.Extra) != "dead extra" {
		t.Fatalf("redriven state was not preserved: %#v", redriven)
	}
	if redriven.VisibilityTimeout != input.VisibilityTimeout {
		t.Fatalf("redriven visibility timeout = %s", redriven.VisibilityTimeout)
	}
	if err := q.Ack(ctx, redriven); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Reservations are intentionally permanent, including after Ack.
	assertBatchItemError(t, q.Push(ctx, &queue.Message{ID: id, QueueName: queueName}), queue.ErrAlreadyExists)
}

func popOneIntegration(t *testing.T, ctx context.Context, q *Queue, queueName string) *queue.Message {
	t.Helper()
	messages, err := q.Pop(ctx, queueName, 1)
	if err != nil {
		t.Fatalf("Pop(%q): %v", queueName, err)
	}
	if len(messages) != 1 {
		t.Fatalf("Pop(%q) returned %d messages", queueName, len(messages))
	}
	return messages[0]
}

func assertBatchItemError(t *testing.T, err, target error) {
	t.Helper()
	var batch *queue.BatchError
	if !errors.As(err, &batch) || len(batch.Failed) != 1 {
		t.Fatalf("error = %#v, want one-item BatchError", err)
	}
	if !errors.Is(batch.Failed[0].Err, target) {
		t.Fatalf("item error = %v, want %v", batch.Failed[0].Err, target)
	}
}
