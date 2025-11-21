package noop

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	messaging "github.com/devlibx/gox-messaging/v2"
)

type noOp struct {
}

func (n *noOp) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	return nil
}

func (n *noOp) Stop() error {
	return nil
}

func (n *noOp) Send(ctx context.Context, message *messaging.Message) chan *messaging.Response {
	ch := make(chan *messaging.Response, 2)
	ch <- &messaging.Response{RawPayload: gox.StringObjectMap{}, Err: nil}
	return ch
}

func NewNoOpConsumer() (messaging.Consumer, error) {
	return &noOp{}, nil
}

func NewNoOpProducer() (messaging.Producer, error) {
	return &noOp{}, nil
}
