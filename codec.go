package rabbitmqqueue

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/go-hypercube/go-hypercube/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

const envelopeVersion = 1

type envelope struct {
	Version           int    `json:"version"`
	ID                []byte `json:"id"`
	QueueName         []byte `json:"queue_name"`
	Namespace         []byte `json:"namespace"`
	JobName           []byte `json:"job_name"`
	Delay             int64  `json:"delay"`
	Payload           []byte `json:"payload"`
	Extra             []byte `json:"extra"`
	Attempt           int64  `json:"attempt"`
	VisibilityTimeout int64  `json:"visibility_timeout"`
}

func envelopeFromMessage(msg *queue.Message, attempt int) envelope {
	return envelope{
		Version:           envelopeVersion,
		ID:                []byte(msg.ID),
		QueueName:         []byte(msg.QueueName),
		Namespace:         []byte(msg.Namespace),
		JobName:           []byte(msg.JobName),
		Delay:             int64(msg.Delay),
		Payload:           cloneBytes(msg.Payload),
		Extra:             cloneBytes(msg.Extra),
		Attempt:           int64(attempt),
		VisibilityTimeout: int64(msg.VisibilityTimeout),
	}
}

func (e envelope) message() (*queue.Message, error) {
	if e.Version != envelopeVersion {
		return nil, fmt.Errorf("rabbitmq queue: unsupported envelope version %d", e.Version)
	}
	if e.Attempt < 0 || e.Attempt > int64(maxInt()) {
		return nil, fmt.Errorf("rabbitmq queue: invalid stored attempt %d", e.Attempt)
	}
	return &queue.Message{
		ID:                string(e.ID),
		QueueName:         string(e.QueueName),
		Namespace:         string(e.Namespace),
		JobName:           string(e.JobName),
		Delay:             time.Duration(e.Delay),
		Payload:           cloneBytes(e.Payload),
		Extra:             cloneBytes(e.Extra),
		Attempt:           int(e.Attempt),
		VisibilityTimeout: time.Duration(e.VisibilityTimeout),
	}, nil
}

func encodeEnvelope(e envelope) ([]byte, error) {
	body, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq queue: encode message: %w", err)
	}
	return body, nil
}

func decodeEnvelope(body []byte) (envelope, error) {
	var e envelope
	if err := json.Unmarshal(body, &e); err != nil {
		return envelope{}, fmt.Errorf("rabbitmq queue: decode message: %w", err)
	}
	if e.Version != envelopeVersion {
		return envelope{}, fmt.Errorf("rabbitmq queue: unsupported envelope version %d", e.Version)
	}
	if e.Attempt < 0 {
		return envelope{}, fmt.Errorf("rabbitmq queue: invalid stored attempt %d", e.Attempt)
	}
	return e, nil
}

func deliveryMessage(body []byte, headers amqp.Table) (*queue.Message, error) {
	e, err := decodeEnvelope(body)
	if err != nil {
		return nil, err
	}
	msg, err := e.message()
	if err != nil {
		return nil, err
	}

	attempt := e.Attempt
	if acquired, ok := integerHeader(headers, "x-acquired-count"); ok {
		// RabbitMQ 4.3+ counts broker acquisitions. Be defensive if a broker
		// reports zero on the first acquisition.
		if acquired < 1 {
			acquired = 1
		}
		if attempt > math.MaxInt64-acquired {
			return nil, fmt.Errorf("rabbitmq queue: attempt counter overflow")
		}
		attempt += acquired
	} else {
		redeliveries, ok := integerHeader(headers, "x-delivery-count")
		if !ok {
			redeliveries = 0
		}
		if redeliveries < 0 || attempt > math.MaxInt64-redeliveries-1 {
			return nil, fmt.Errorf("rabbitmq queue: invalid delivery counter %d", redeliveries)
		}
		attempt += redeliveries + 1
	}
	if attempt > int64(maxInt()) {
		return nil, fmt.Errorf("rabbitmq queue: attempt counter %d overflows int", attempt)
	}

	msg.Attempt = int(attempt)
	msg.Delay = 0
	msg.DriverData = nil
	return msg, nil
}

func integerHeader(headers amqp.Table, name string) (int64, bool) {
	if headers == nil {
		return 0, false
	}
	value, ok := headers[name]
	if !ok {
		return 0, false
	}
	switch n := value.(type) {
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		if n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	case int:
		return int64(n), true
	case uint:
		if uint64(n) > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	default:
		return 0, false
	}
}

func cloneMessage(msg *queue.Message) queue.Message {
	cloned := *msg
	cloned.Payload = cloneBytes(msg.Payload)
	cloned.Extra = cloneBytes(msg.Extra)
	cloned.DriverData = nil
	return cloned
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte{}, value...)
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
