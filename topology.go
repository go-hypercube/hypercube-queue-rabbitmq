package rabbitmqqueue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func physicalNamespace(prefix string) string {
	sum := sha256.Sum256([]byte(prefix))
	return "hcq." + hex.EncodeToString(sum[:])
}

func nameDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func (q *Queue) workQueueName(logicalName string) string {
	return q.namespace + ".q." + nameDigest(logicalName)
}

func (q *Queue) deadQueueName(logicalName string) string {
	return q.namespace + ".dead." + nameDigest(logicalName)
}

func (q *Queue) markerQueueName(id string) string {
	return q.namespace + ".id." + nameDigest(id)
}

func (q *Queue) delayQueueName(logicalName string, milliseconds int64) string {
	return fmt.Sprintf("%s.delay.%s.%d", q.namespace, nameDigest(logicalName), milliseconds)
}

func delayMilliseconds(delay time.Duration) (int64, error) {
	if delay <= 0 {
		return 0, nil
	}
	milliseconds := int64(delay / time.Millisecond)
	if delay%time.Millisecond != 0 {
		milliseconds++
	}
	return milliseconds, nil
}

func (q *Queue) declareWorkQueue(ch *amqp.Channel, logicalName string) error {
	_, err := ch.QueueDeclare(
		q.workQueueName(logicalName),
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-queue-type":     "quorum",
			"x-delivery-limit": int32(-1),
		},
	)
	if err != nil {
		return fmt.Errorf("rabbitmq queue: declare work queue %q: %w", logicalName, err)
	}
	return nil
}

func (q *Queue) declareDeadQueue(ch *amqp.Channel, logicalName string) error {
	_, err := ch.QueueDeclare(
		q.deadQueueName(logicalName),
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-queue-type":     "quorum",
			"x-delivery-limit": int32(-1),
		},
	)
	if err != nil {
		return fmt.Errorf("rabbitmq queue: declare dead queue %q: %w", logicalName, err)
	}
	return nil
}

func (q *Queue) declareDelayQueue(ch *amqp.Channel, logicalName string, milliseconds int64) (string, error) {
	if err := q.declareWorkQueue(ch, logicalName); err != nil {
		return "", err
	}
	name := q.delayQueueName(logicalName, milliseconds)
	_, err := ch.QueueDeclare(
		name,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-queue-type":              "quorum",
			"x-message-ttl":             milliseconds,
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": q.workQueueName(logicalName),
			"x-dead-letter-strategy":    "at-least-once",
			"x-overflow":                "reject-publish",
			"x-delivery-limit":          int32(-1),
		},
	)
	if err != nil {
		return "", fmt.Errorf("rabbitmq queue: declare %s delay queue for %q: %w", time.Duration(milliseconds)*time.Millisecond, logicalName, err)
	}
	return name, nil
}

func (q *Queue) declareMarkerQueue(ch *amqp.Channel, id string) error {
	_, err := ch.QueueDeclare(
		q.markerQueueName(id),
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-queue-type": "classic",
			"x-max-length": int32(1),
			"x-overflow":   "reject-publish",
		},
	)
	if err != nil {
		return fmt.Errorf("rabbitmq queue: declare ID reservation: %w", err)
	}
	return nil
}

func publishMessageConfirmed(ctx context.Context, ch *amqp.Channel, routingKey, id string, body []byte) error {
	return publishConfirmed(
		ctx, ch, routingKey, id, body,
		"application/vnd.go-hypercube.queue-message+json",
		"go-hypercube.queue.message.v1",
	)
}

func publishReservationConfirmed(ctx context.Context, ch *amqp.Channel, routingKey, id string) error {
	return publishConfirmed(
		ctx, ch, routingKey, id, []byte(id),
		"application/octet-stream",
		"go-hypercube.queue.id-reservation.v1",
	)
}

func publishConfirmed(
	ctx context.Context,
	ch *amqp.Channel,
	routingKey string,
	id string,
	body []byte,
	contentType string,
	messageType string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		"",
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  contentType,
			DeliveryMode: amqp.Persistent,
			MessageId:    nameDigest(id),
			Type:         messageType,
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("rabbitmq queue: publish message %q: %w", id, err)
	}
	if confirmation == nil {
		return errors.New("rabbitmq queue: publisher confirms are not enabled")
	}
	acknowledged, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("rabbitmq queue: wait for message %q confirmation: %w", id, err)
	}
	if !acknowledged {
		return fmt.Errorf("%w: message %q", ErrPublishRejected, id)
	}
	return nil
}
