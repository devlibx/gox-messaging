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
	config               messaging.ConsumerConfig
	close                chan bool
	stopDoOnce           sync.Once
	startDoOnce          sync.Once
	logger               *zap.Logger
	consumerCloseCounter sync.WaitGroup
}

func (d *kafkaConsumerV1) String() string {
	return fmt.Sprintf("consumer name=%s topic=%s type=%s", d.config.Name, d.config.Name, d.config.Type)
}

func (k *kafkaConsumerV1) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	k.startDoOnce.Do(func() {
		for i := 0; i < k.config.Concurrency; i++ {
			go func(index int) {
				k.consumerCloseCounter.Add(1)
				k.internalProcess(ctx, k.logger.Named(fmt.Sprintf("%d", index)).Named(k.config.Topic), k.consumers[index], consumeFunction)
				k.consumerCloseCounter.Done()
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
	k.logger.Info("waiting got all consumer threads to be closed")
	k.consumerCloseCounter.Wait()
	k.logger.Info("wait over, all consumer threads are stopped")
	return nil
}

func (k *kafkaConsumerV1) closeConsumer(consumer *kafka.Consumer) {
	_ = consumer.Close()
}

func (k *kafkaConsumerV1) internalProcess(ctx context.Context, logger *zap.Logger, consumer *kafka.Consumer, consumeFunction messaging.ConsumeFunction) {

	// If auto commit is false then we need to commit message after it is consumed
	commit := k.config.Properties.BoolOrTrue(messaging.KMessagingPropertyEnableAutoCommit)
	logNoMessageMod := k.config.Properties.IntOrDefault("log_no_message_mod", 10)
	logNoMessage := k.config.Properties.BoolOrFalse("log_no_message")
	loopCounter := 0

	logger.Info("consumer started", zap.Bool("log_no_message", logNoMessage), zap.Int("log_no_message_mod", logNoMessageMod))
L:
	for {
		loopCounter++
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
			} else if logNoMessage && loopCounter%logNoMessageMod == 0 {
				k.logger.Info("no messages in topic", zap.String("topic", k.config.Name))
			}
		}
	}
	logger.Info("consumer closed [loop exit]")
}

func NewKafkaConsumer(cf gox.CrossFunction, config messaging.ConsumerConfig) (p messaging.Consumer, err error) {

	// Setup defaults if some inputs are missing
	config.SetupDefaults()

	c := kafkaConsumerV1{
		CrossFunction:        cf,
		config:               config,
		close:                make(chan bool, 100),
		stopDoOnce:           sync.Once{},
		startDoOnce:          sync.Once{},
		logger:               cf.Logger().Named("kafka.consumer").Named(config.Name),
		consumerCloseCounter: sync.WaitGroup{},
	}

	c.consumers = make([]*kafka.Consumer, config.Concurrency)
	for i := 0; i < config.Concurrency; i++ {
		c.consumers[i], err = kafka.NewConsumer(
			&kafka.ConfigMap{
				"bootstrap.servers":  config.Endpoint,
				"group.id":           config.Properties["group.id"],
				"auto.offset.reset":  config.Properties["auto.offset.reset"],
				"session.timeout.ms": config.Properties.IntOrDefault(messaging.KMessagingPropertySessionTimeoutMs, 10000),
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
