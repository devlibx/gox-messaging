package dummy

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	messaging "github.com/devlibx/gox-messaging/v2"
	"go.uber.org/zap"
	"sync"
	"time"
)

type dummyConsumer struct {
	queue  chan *messaging.Message
	doOnce sync.Once
	config messaging.ConsumerConfig
	logger *zap.Logger
	close  bool
}

func (d *dummyConsumer) String() string {
	return fmt.Sprintf("consumer name=%s topic=%s type=%s", d.config.Name, d.config.Name, d.config.Type)
}

func (d *dummyConsumer) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	go func() {
	L:
		for {
			select {
			case ev, ok := <-dummyChannels[d.config.Name]:
				if ok {
					err := consumeFunction.Process(ev)
					if err != nil {
						d.logger.Error("error in processing message", zap.Any("message", ev), zap.Error(err))
					}
				} else {
					break L
				}
			case <-time.After(10 * time.Millisecond):
				if d.close {
					break L
				}
			}
		}
	}()
	return nil
}

func (d *dummyConsumer) Stop() error {
	d.close = true
	return nil
}

func NewDummyConsumer(cf gox.CrossFunction, config messaging.ConsumerConfig) messaging.Consumer {
	p := &dummyConsumer{
		queue:  make(chan *messaging.Message, 100),
		doOnce: sync.Once{},
		config: config,
		logger: cf.Logger().Named("dummy.consumer").Named(config.Name).Named(config.Topic),
		close:  false,
	}
	dummyConsumers[config.Name] = p
	return p
}
