package gox_messaging

import (
	"context"
	errors "errors"
)

var ErrProducerNotFound = errors.New("produce not found")
var ErrProducerClosed = errors.New("produce is closed")
var ErrConsumerNotFound = errors.New("consumer not found")

// Provides producer and consumers
type Factory interface {
	Start(configuration Configuration) error
	GetProducer(name string) (Producer, error)
	GetConsumer(name string) (Consumer, error)
	RegisterProducer(config ProducerConfig) error
	RegisterConsumer(config ConsumerConfig) error
	Stop() error
}

// Provides client with a capability to produce a message
type Producer interface {
	Send(ctx context.Context, key string, data []byte) chan error
	Stop() error
}

// Message for consumption
type Message struct {
	Key  string
	Data []byte
}

// Consumer function which is called for each message
type ConsumerFunc func(message *Message) error

// Setup a consumer
type Consumer interface {
	Start(consumerFunc ConsumerFunc) error
	Stop() error
}
