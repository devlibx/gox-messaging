package factory

import (
	"context"
	"github.com/devlibx/gox-base"
	messaging "github.com/devlibx/gox-messaging"
)

type noOpFactory struct {
}

func (n *noOpFactory) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	return nil
}

func (n *noOpFactory) Send(ctx context.Context, message *messaging.Message) chan *messaging.Response {
	ch := make(chan *messaging.Response, 2)
	ch <- &messaging.Response{RawPayload: gox.StringObjectMap{}, Err: nil}
	return ch
}

func (n *noOpFactory) MarkStart() {
}

func (n *noOpFactory) Start(configuration messaging.Configuration) error {
	return nil
}

func (n *noOpFactory) GetProducer(name string) (messaging.Producer, error) {
	return n, nil
}

func (n *noOpFactory) GetConsumer(name string) (messaging.Consumer, error) {
	return n, nil
}

func (n *noOpFactory) RegisterProducer(config messaging.ProducerConfig) error {
	return nil
}

func (n *noOpFactory) RegisterConsumer(config messaging.ConsumerConfig) error {
	return nil
}

func (n noOpFactory) Stop() error {
	return nil
}

func NewNoOpMessagingFactory() messaging.Factory {
	return &noOpFactory{}
}
