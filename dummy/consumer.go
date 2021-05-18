package dummy

import (
	"context"
	"github.com/devlibx/gox-base"
	messaging "github.com/devlibx/gox-messaging"
	"go.uber.org/zap"
	"sync"
)

type dummyConsumer struct {
	queue  chan *messaging.Message
	doOnce sync.Once
	config messaging.ConsumerConfig
	logger *zap.Logger
}

func (d *dummyConsumer) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	go func() {
		for ev := range dummyChannels[d.config.Name] {
			err := consumeFunction.Process(ev)
			if err != nil {
				d.logger.Error("error in processing message", zap.Any("message", ev), zap.Error(err))
			}
		}
	}()
	return nil
}

func (d *dummyConsumer) Stop() error {
	return nil
}

func NewDummyConsumer(cf gox.CrossFunction, config messaging.ConsumerConfig) messaging.Consumer {
	p := &dummyConsumer{
		queue:  make(chan *messaging.Message, 100),
		doOnce: sync.Once{},
		config: config,
		logger: cf.Logger().Named("dummy.consumer").Named(config.Name).Named(config.Topic),
	}
	dummyConsumers[config.Name] = p
	return p
}
