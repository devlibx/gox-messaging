package messaging

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

// Message for consumption
type Message struct {
	Key  string
	Data []byte
}

type Event struct {
	Key      string
	Value    interface{}
	RawEvent interface{}
}

type Response struct {
	RawPayload interface{}
}

// Provides client with a capability to produce a message
type Producer interface {
	Send(request *Event) (*Response, error)
	Stop() error
}

// Consumer function which is called for each message
type Consumer interface {
	Process(ctx context.Context, messagePressedAckChannel chan Event) (chan Event, error)
}
