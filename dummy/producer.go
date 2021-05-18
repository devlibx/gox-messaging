package dummy

import (
	"context"
	"github.com/devlibx/gox-base"
	messaging "github.com/devlibx/gox-messaging"
	"go.uber.org/zap"
	"sync"
)

var dummyProducers = map[string]*dummyProducerV1{}
var dummyConsumers = map[string]*dummyConsumer{}
var dummyChannels = make(map[string]chan *messaging.Message)

type dummyProducerV1 struct {
	queue  chan *messaging.Message
	doOnce sync.Once
	config messaging.ProducerConfig
	logger *zap.Logger
}

func (d *dummyProducerV1) Send(ctx context.Context, message *messaging.Message) chan *messaging.Response {
	d.queue <- message
	d.logger.Debug("sent message from dummy producer", zap.Any("message", message))
	responseChannel := make(chan *messaging.Response, 1)
	responseChannel <- &messaging.Response{RawPayload: ""}
	close(responseChannel)
	return responseChannel
}

func (d *dummyProducerV1) Stop() error {
	d.doOnce.Do(func() {
		close(d.queue)
	})
	return nil
}

func NewDummyProducer(cf gox.CrossFunction, config messaging.ProducerConfig) messaging.Producer {
	p := &dummyProducerV1{
		queue:  make(chan *messaging.Message, 100),
		doOnce: sync.Once{},
		logger: cf.Logger().Named("dummy.producer").Named(config.Name).Named(config.Topic),
	}
	dummyProducers[config.Name] = p
	dummyChannels[config.Name] = p.queue
	return p
}
