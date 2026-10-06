package rabbitmqqueue

import "errors"

var (
	// ErrClosed is returned after the queue has been closed.
	ErrClosed = errors.New("rabbitmq queue: closed")

	// ErrPublishRejected indicates that RabbitMQ negatively acknowledged a
	// publication. The original delivery is left unsettled when possible.
	ErrPublishRejected = errors.New("rabbitmq queue: publication rejected")
)
