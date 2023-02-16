package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/devlibx/gox-base"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/pkg/errors"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
	"hash/fnv"
	"math"
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
	rateLimiter          ratelimit.Limiter

	messageSubChannel              []chan subChannelMsg
	partitionProcessingParallelism int
	messageSubChannelDoOnce        *sync.Once
	messageSubChannelCloseDoOnce   *sync.Once
}

type subChannelMsg struct {
	consumeFunction messaging.ConsumeFunction
	message         *messaging.Message
	autoCommit      bool
	consumer        *kafka.Consumer
	kafkaRawMsg     *kafka.Message
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

	// Close all message sub channels to make sure all go routines are closed
	k.messageSubChannelCloseDoOnce.Do(func() {
		for i := 0; i < k.partitionProcessingParallelism; i++ {
			close(k.messageSubChannel[i])
		}
	})
}

func (k *kafkaConsumerV1) internalProcess(ctx context.Context, logger *zap.Logger, consumer *kafka.Consumer, consumeFunction messaging.ConsumeFunction) {

	// If auto commit is false then we need to commit message after it is consumed
	autoCommit := k.config.Properties.BoolOrTrue(messaging.KMessagingPropertyEnableAutoCommit)
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

			// Apply rate limiting if applicable
			if k.rateLimiter != nil {
				k.rateLimiter.Take()
			}

			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err == nil {
				message := &messaging.Message{
					Key:              string(msg.Key),
					Payload:          msg.Value,
					KafkaMessageInfo: messaging.KafkaMessageInfo{TopicPartition: msg.TopicPartition},
				}

				if k.messageSubChannel == nil || len(k.messageSubChannel) == 0 {
					_ = k.processSingleMessage(consumeFunction, message, autoCommit, consumer, msg)
				} else {
					_ = k.processSingleMessageInSubChannel(consumeFunction, message, autoCommit, consumer, msg)
				}

			} else if logNoMessage && loopCounter%logNoMessageMod == 0 {
				k.logger.Info("no messages in topic", zap.String("topic", k.config.Name))
			}
		}
	}
	logger.Info("consumer closed [loop exit]")
}

func (k *kafkaConsumerV1) processSingleMessage(consumeFunction messaging.ConsumeFunction, message *messaging.Message, autoCommit bool, consumer *kafka.Consumer, kafkaRawMsg *kafka.Message) (err error) {
	err = consumeFunction.Process(message)
	if err != nil {
		consumeFunction.ErrorInProcessing(message, err)
		k.Metric().Tagged(map[string]string{"type": "kafka", "topic": k.config.Topic, "status": "error", "error": "consumer_error"}).Counter("message_consumed").Inc(1)
	} else {
		k.Metric().Tagged(map[string]string{"type": "kafka", "topic": k.config.Topic, "status": "ok", "error": "na"}).Counter("message_consumed").Inc(1)
	}
	if !autoCommit {
		if _, err = consumer.CommitMessage(kafkaRawMsg); err != nil {
			k.logger.Error("failed to commit message", zap.String("key", string(kafkaRawMsg.Key)))
			k.Metric().Tagged(map[string]string{"type": "kafka", "topic": k.config.Topic, "status": "ok"}).Counter("message_consumed_offset_commit").Inc(1)
		} else {
			k.Metric().Tagged(map[string]string{"type": "kafka", "topic": k.config.Topic, "status": "error"}).Counter("message_consumed_offset_commit").Inc(1)
		}
	}
	return err
}

func (k *kafkaConsumerV1) processSingleMessageInSubChannel(consumeFunction messaging.ConsumeFunction, message *messaging.Message, autoCommit bool, consumer *kafka.Consumer, kafkaRawMsg *kafka.Message) (err error) {

	// Setup go routines to process messages in parallel
	k.messageSubChannelDoOnce.Do(func() {
		for i := 0; i < k.partitionProcessingParallelism; i++ {
			go func(index int) {
				for m := range k.messageSubChannel[index] {
					_ = k.processSingleMessage(m.consumeFunction, m.message, m.autoCommit, m.consumer, m.kafkaRawMsg)
				}
			}(i)
		}
	})

	// Find the sub channel based on the message hash
	// Have a safe check to make sure we do not go out of bound
	partition := StringToHashMod(message.Key, k.partitionProcessingParallelism)
	if partition < 0 || partition >= len(k.messageSubChannel) {
		partition = 0
	}

	// Put the message to resected channel for processing
	k.messageSubChannel[partition] <- subChannelMsg{
		consumeFunction: consumeFunction,
		message:         message,
		autoCommit:      autoCommit,
		consumer:        consumer,
		kafkaRawMsg:     kafkaRawMsg,
	}

	return nil
}

func NewKafkaConsumer(cf gox.CrossFunction, config messaging.ConsumerConfig) (p messaging.Consumer, err error) {

	// Setup defaults if some inputs are missing
	config.SetupDefaults()

	c := kafkaConsumerV1{
		CrossFunction:                cf,
		config:                       config,
		close:                        make(chan bool, 100),
		stopDoOnce:                   sync.Once{},
		startDoOnce:                  sync.Once{},
		logger:                       cf.Logger().Named("kafka.consumer").Named(config.Name),
		consumerCloseCounter:         sync.WaitGroup{},
		messageSubChannelDoOnce:      &sync.Once{},
		messageSubChannelCloseDoOnce: &sync.Once{},
	}

	c.consumers = make([]*kafka.Consumer, config.Concurrency)
	for i := 0; i < config.Concurrency; i++ {

		cm := &kafka.ConfigMap{
			"bootstrap.servers":  config.Endpoint,
			"group.id":           config.Properties["group.id"],
			"auto.offset.reset":  config.Properties["auto.offset.reset"],
			"session.timeout.ms": config.Properties.IntOrDefault(messaging.KMessagingPropertySessionTimeoutMs, 10000),
			"enable.auto.commit": config.Properties.BoolOrTrue(messaging.KMessagingPropertyEnableAutoCommit),
		}

		// Pass-through property
		if val, ok := config.Properties[messaging.KMessagingKafkaSpecificProperties]; ok {
			if mapVal, ok := val.(map[string]interface{}); ok {
				for k, v := range mapVal {
					if err = cm.SetKey(k, v); err != nil {
						return nil, errors.Wrapf(err, "failed to set consumer property: name=%s, value=%v", k, v)
					}
				}
			} else if sMapVal, ok := config.Properties[messaging.KMessagingKafkaSpecificProperties].(gox.StringObjectMap); ok {
				for k, v := range sMapVal {
					if err = cm.SetKey(k, v); err != nil {
						return nil, errors.Wrapf(err, "failed to set consumer property: name=%s, value=%v", k, v)
					}
				}
			}
		}

		c.consumers[i], err = kafka.NewConsumer(cm)
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

		// Set a rate limit for consumer
		rateLimitValue := config.Properties.IntOrDefault(messaging.KMessagingPropertyRateLimitPerSec, 0)
		if rateLimitValue > 0 {
			c.rateLimiter = ratelimit.New(rateLimitValue)
		}

		// Set a PartitionProcessingParallelism for consumer
		c.partitionProcessingParallelism = config.Properties.IntOrDefault(messaging.KMessagingPropertyPartitionProcessingParallelism, 0)
		if c.partitionProcessingParallelism > 0 {
			c.messageSubChannel = make([]chan subChannelMsg, c.partitionProcessingParallelism)
			for i := 0; i < c.partitionProcessingParallelism; i++ {
				c.messageSubChannel[i] = make(chan subChannelMsg, 3)
			}
		}
	}

	return &c, nil
}

func StringToHashMod(item string, count int) int {
	h := fnv.New64()
	_, _ = h.Write([]byte(item))
	sha := h.Sum64() % uint64(count)
	return int(math.Abs(float64(sha)))
}
