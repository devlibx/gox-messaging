package kafka

import (
	"context"
	"fmt"
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
	logger      *zap.Logger
}

func (d *kafkaConsumerV1) String() string {
	return fmt.Sprintf("consumer name=%s topic=%s type=%s", d.config.Name, d.config.Name, d.config.Type)
}

func (k *kafkaConsumerV1) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	k.startDoOnce.Do(func() {
		for i := 0; i < k.config.Concurrency; i++ {
			go func(index int) {
				k.internalProcess(ctx, k.logger.Named(fmt.Sprintf("%d", index)).Named(k.config.Topic), k.consumers[index], consumeFunction)
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

func (k *kafkaConsumerV1) internalProcess(ctx context.Context, logger *zap.Logger, consumer *kafka.Consumer, consumeFunction messaging.ConsumeFunction) {

	// If auto commit is false then we need to commit message after it is consumed
	commit := false
	if val, ok := k.config.Properties[messaging.KMessagingPropertyEnableAutoCommit]; ok {
		if val == "false" {
			commit = true
		}
	}

	logger.Info("consumer started")
L:
	for {
		select {
		case <-ctx.Done():
			k.closeConsumer(consumer)
			logger.Info("close consumer [cause context done]")
			break L
		case <-k.close:
			k.closeConsumer(consumer)
			logger.Info("close consumer [cause explicit close]")
			break L
		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err == nil {
				message := &messaging.Message{
					Key:     string(msg.Key),
					Payload: msg.Value,
				}
				err = consumeFunction.Process(message)
				if err != nil {
					consumeFunction.ErrorInProcessing(message, err)
				}
				if commit {
					if _, err = consumer.CommitMessage(msg); err != nil {
						k.logger.Error("failed to commit message", zap.String("key", string(msg.Key)))
					}
				}
			}
		}
	}
}

func NewKafkaConsumer(cf gox.CrossFunction, config messaging.ConsumerConfig) (p messaging.Consumer, err error) {

	// Setup defaults if some inputs are missing
	config.SetupDefaults()

	c := kafkaConsumerV1{
		CrossFunction: cf,
		config:        config,
		close:         make(chan bool, 1),
		stopDoOnce:    sync.Once{},
		startDoOnce:   sync.Once{},
		logger:        cf.Logger().Named("kafka.consumer").Named(config.Name),
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

		index := i
		err = c.consumers[i].Subscribe(config.Topic, func(consumer *kafka.Consumer, event kafka.Event) error {
			c.logger.With(zap.Int("index", index)).Info("consumer subscribe callback", zap.Any("event", event))
			return nil
		})
		if err != nil {
			return nil, errors.Wrap(err, "failed to subscribe consumer: name="+config.Name)
		}
	}

	return &c, nil
}
