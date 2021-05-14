package messaging

import (
	"context"
	"github.com/devlibx/gox-base/errors"
	"github.com/devlibx/gox-base/serialization"
	"time"
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
	Key                              string
	Payload                          interface{}
	ArtificialDelayToSimulateLatency time.Duration
}

func (m *Message) PayloadAsString() (string, error) {
	if m.Payload == nil {
		return "", nil
	} else if data, ok := m.Payload.(string); ok {
		return data, nil
	} else if data, err := serialization.Stringify(m.Payload); err != nil {
		return "", errors.Wrap(err, "failed to creat string from message: request=%v", m)
	} else {
		return data, nil
	}
}

type Event struct {
	Key      string
	Value    interface{}
	RawEvent interface{}
}

type Response struct {
	RawPayload interface{}
	Err        error

	// DO not use it
	ResultChannel chan error
}

// Provides client with a capability to produce a message
type Producer interface {
	Send(request *Event) (*Response, error)
}

// Consumer function which is called for each message
type Consumer interface {
	Process(ctx context.Context, messagePressedAckChannel chan Event) (chan Event, error)
}

// Provides client with a capability to produce a message
type ProducerV1 interface {
	Send(ctx context.Context, message *Message) chan *Response
	Stop() error
}

type ConsumeFunc func(message *Message) error

// Consumer function which is called for each message
type ConsumerV1 interface {
	Process(ctx context.Context, consumeFunction ConsumeFunction) error
	Stop() error
}

type ConsumeFunction interface {
	Process(message *Message) error
	ErrorInProcessing(message *Message, err error)
}
