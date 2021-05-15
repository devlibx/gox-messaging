package kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/devlibx/gox-base"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"sync"
	"time"
)

type kafkaConsumerV1 struct {
	consumers []*kafka.Consumer
	gox.CrossFunction
	config      messaging.ConsumerConfig
	close       chan bool
	stopDoOnce  sync.Once
	startDoOnce sync.Once
}

func (k *kafkaConsumerV1) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	k.stopDoOnce.Do(func() {
		for i := 0; i < k.config.Concurrency; i++ {
			go func(index int) {
				k.internalProcess(ctx, k.consumers[index], consumeFunction)
			}(i)
		}
	})
	return nil
}

func (k *kafkaConsumerV1) Stop() error {
	k.stopDoOnce.Do(func() {
		k.close <- true
		close(k.close)
	})
	return nil
}

func (k *kafkaConsumerV1) closeConsumer(consumer *kafka.Consumer) {
	_ = consumer.Close()
}

func (k *kafkaConsumerV1) internalProcess(ctx context.Context, consumer *kafka.Consumer, consumeFunction messaging.ConsumeFunction) {
L:
	for {
		select {
		case <-ctx.Done():
			k.closeConsumer(consumer)
			break L
		case <-k.close:
			k.closeConsumer(consumer)
			break L
		default:
			msg, err := consumer.ReadMessage(100)
			if err == nil {
				message := &messaging.Message{
					Key:     string(msg.Key),
					Payload: msg.Value,
				}
				err = consumeFunction.Process(message)
				if err != nil {
					consumeFunction.ErrorInProcessing(message, err)
				}

			} else {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
}

func newKafkaConsumerV1(cf gox.CrossFunction, config messaging.ConsumerConfig) (p messaging.ConsumerV1, err error) {

	// Setup defaults if some inputs are missing
	config.SetupDefaults()

	c := kafkaConsumerV1{
		CrossFunction: cf,
		config:        config,
		close:         make(chan bool, 1),
		stopDoOnce:    sync.Once{},
		startDoOnce:   sync.Once{},
	}

	c.consumers = make([]*kafka.Consumer, config.Concurrency)
	for i := 0; i < config.Concurrency; i++ {
		c.consumers[i], err = kafka.NewConsumer(
			&kafka.ConfigMap{
				"bootstrap.servers": config.Endpoint,
				"group.id":          config.Properties["group.id"],
				"auto.offset.reset": config.Properties["auto.offset.reset"],
			},
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create consumer: name="+config.Name)
		}

		err = c.consumers[i].Subscribe(config.Topic, func(consumer *kafka.Consumer, event kafka.Event) error {
			c.Logger().Info("consumer RebalanceCb callback", zap.Any("event", event))
			return nil
		})
		if err != nil {
			return nil, errors.Wrap(err, "failed to subscribe consumer: name="+config.Name)
		}
	}

	return &c, nil
}
