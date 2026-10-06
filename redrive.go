package rabbitmqqueue

import (
	"context"
	"fmt"

	"github.com/go-hypercube/go-hypercube/queue"
)

// Redrive moves up to count explicit dead letters back to the active queue.
// The active publication is confirmed before the dead delivery is acknowledged.
func (q *Queue) Redrive(ctx context.Context, queueName string, count int) (int, error) {
	if count <= 0 {
		return 0, queue.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	ch, err := q.openChannel(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = ch.Close() }()
	if err := ch.Confirm(false); err != nil {
		return 0, fmt.Errorf("rabbitmq queue: enable redrive publisher confirms: %w", err)
	}
	if err := q.declareWorkQueue(ch, queueName); err != nil {
		return 0, err
	}
	if err := q.declareDeadQueue(ch, queueName); err != nil {
		return 0, err
	}

	deadName := q.deadQueueName(queueName)
	workName := q.workQueueName(queueName)
	completed := 0
	for completed < count {
		if err := ctx.Err(); err != nil {
			return completed, err
		}
		delivery, ok, err := ch.Get(deadName, false)
		if err != nil {
			return completed, fmt.Errorf("rabbitmq queue: get dead message from %q: %w", queueName, err)
		}
		if !ok {
			return completed, nil
		}

		e, err := decodeEnvelope(delivery.Body)
		if err != nil {
			_ = ch.Nack(delivery.DeliveryTag, false, true)
			return completed, err
		}
		if string(e.QueueName) != queueName {
			_ = ch.Nack(delivery.DeliveryTag, false, true)
			return completed, fmt.Errorf(
				"rabbitmq queue: dead message %q belongs to queue %q, not %q",
				string(e.ID), string(e.QueueName), queueName,
			)
		}
		e.Attempt = 0
		e.Delay = 0
		body, err := encodeEnvelope(e)
		if err != nil {
			_ = ch.Nack(delivery.DeliveryTag, false, true)
			return completed, err
		}
		if err := publishMessageConfirmed(ctx, ch, workName, string(e.ID), body); err != nil {
			_ = ch.Nack(delivery.DeliveryTag, false, true)
			return completed, err
		}
		if err := ch.Ack(delivery.DeliveryTag, false); err != nil {
			return completed, fmt.Errorf("rabbitmq queue: acknowledge redriven message %q: %w", string(e.ID), err)
		}
		if _, err := ch.QueueInspect(deadName); err != nil {
			return completed, fmt.Errorf("rabbitmq queue: confirm redrive acknowledgement for %q: %w", string(e.ID), err)
		}
		completed++
	}
	return completed, nil
}
