package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/devlibx/gox-base"
	errors2 "github.com/devlibx/gox-base/errors"
	"github.com/devlibx/gox-base/util"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"sync"
	"time"
)

type kafkaProducerV1 struct {
	*kafka.Producer
	gox.CrossFunction
	config           messaging.ProducerConfig
	messageQueue     chan *internalSendMessage
	close            chan bool
	internalSendFunc func(internalSendMessage *internalSendMessage)
	stopDoOnce       sync.Once
	logger           *zap.Logger
}

func (d *kafkaProducerV1) String() string {
	return fmt.Sprintf("producer name=%s topic=%s type=%s", d.config.Name, d.config.Name, d.config.Type)
}

type internalSendMessage struct {
	message         *messaging.Message
	ctx             context.Context
	responseChannel chan *messaging.Response
}

func (k *kafkaProducerV1) Send(ctx context.Context, message *messaging.Message) chan *messaging.Response {
	responseChannel := make(chan *messaging.Response, 1)
	select {
	case closeMessageChannel, _ := <-k.close:
		if closeMessageChannel {
			k.logger.Info("close the producer internal channel")
			close(k.messageQueue)
		}
		responseChannel <- &messaging.Response{Err: errors2.New("producer is closed: name=%s", k.config.Name)}
		close(responseChannel)

	case <-ctx.Done():
		responseChannel <- &messaging.Response{Err: errors2.Wrap(ctx.Err(), "context is closed to sqs producer: name=%s, message=%s", k.config.Name, message.Key)}
		close(responseChannel)

	default:
		k.messageQueue <- &internalSendMessage{ctx: ctx, message: message, responseChannel: responseChannel}
	}
	return responseChannel
}

func (k *kafkaProducerV1) Stop() error {
	k.stopDoOnce.Do(func() {
		k.close <- true
		close(k.close)
		k.Send(context.Background(), &messaging.Message{Key: "", Payload: ""})
	})
	return nil
}

func (k *kafkaProducerV1) internalSendWork() {
	for msg := range k.messageQueue {
		k.internalSendFunc(msg)
	}
	k.Close()
	k.logger.Info("closed producer")
}

func NewKafkaProducer(cf gox.CrossFunction, config messaging.ProducerConfig) (p messaging.Producer, err error) {

	// Setup defaults if some inputs are missing
	config.SetupDefaults()

	kp := &kafkaProducerV1{
		config:        config,
		close:         make(chan bool, 1),
		stopDoOnce:    sync.Once{},
		messageQueue:  make(chan *internalSendMessage, config.MaxMessageInBuffer),
		CrossFunction: cf,
		logger:        cf.Logger().Named("kafka.producer").Named(config.Name).Named(config.Topic),
	}

	// Make a new kafka producer
	kp.Producer, err = kafka.NewProducer(
		&kafka.ConfigMap{
			"bootstrap.servers":   config.Endpoint,
			"acks":                config.Properties["acks"],
			"go.delivery.reports": config.Properties[messaging.KMessagingPropertyDisableDeliveryReports],
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create producer: name="+config.Name)
	}

	// Setup send functions
	if kp.config.Async {
		kp.internalSendFunc = createAsyncInternalSendFuncV1(kp)
		go func() {
			for ev := range kp.Producer.Events() {
				if ev != nil && !util.IsStringEmpty(ev.String()) {
					kp.logger.Debug("error in async message sent", zap.String("topic", kp.config.Topic), zap.String("errStr", ev.String()))
				}
			}
		}()
	} else {
		kp.internalSendFunc = createSyncInternalSendFuncV1(kp)
	}

	// Start send worker
	go kp.internalSendWork()

	return kp, nil
}

func createAsyncInternalSendFuncV1(k *kafkaProducerV1) func(internalSendMessage *internalSendMessage) {
	return func(internalSendMessage *internalSendMessage) {

		// Get payload as bytes
		payload, err := internalSendMessage.message.PayloadAsBytes()
		if err != nil {
			internalSendMessage.responseChannel <- &messaging.Response{Err: errors2.Wrap(err, "failed to send sync kafka message - cannot read bytes")}
			return
		}

		// Send message via producer
		err = k.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &k.config.Topic, Partition: kafka.PartitionAny},
			Value:          payload,
			Key:            []byte(internalSendMessage.message.Key),
		}, nil)
		if err != nil {
			internalSendMessage.responseChannel <- &messaging.Response{Err: errors2.Wrap(err, "failed to send sync kafka message")}
		} else {
			internalSendMessage.responseChannel <- &messaging.Response{RawPayload: ""}
			k.logger.Debug("message sent", zap.String("topic", k.config.Topic), zap.String("key", internalSendMessage.message.Key))
		}
	}
}

func createSyncInternalSendFuncV1(k *kafkaProducerV1) func(internalSendMessage *internalSendMessage) {
	return func(internalSendMessage *internalSendMessage) {

		// Get payload as bytes
		payload, err := internalSendMessage.message.PayloadAsBytes()
		if err != nil {
			internalSendMessage.responseChannel <- &messaging.Response{Err: errors2.Wrap(err, "failed to send sync kafka message - cannot read bytes")}
			return
		}

		// Send message via kafka producer
		deliveryChan := make(chan kafka.Event, 1)
		if err := k.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &k.config.Topic, Partition: kafka.PartitionAny},
			Value:          payload,
			Key:            []byte(internalSendMessage.message.Key),
		}, deliveryChan); err != nil {
			internalSendMessage.responseChannel <- &messaging.Response{Err: errors2.Wrap(err, "failed to send message to kafka")}
			return
		}

		// Use delivery channel to see if we got response
		select {
		case status, _ := <-deliveryChan:
			if ev, ok := status.(*kafka.Message); ok {
				if ev.TopicPartition.Error == nil {
					internalSendMessage.responseChannel <- &messaging.Response{RawPayload: ev}
					k.logger.Debug(">> [sync message out]", zap.String("topic", k.config.Topic), zap.String("key", internalSendMessage.message.Key))
				} else {
					internalSendMessage.responseChannel <- &messaging.Response{Err: errors2.Wrap(ev.TopicPartition.Error, "failed to produce message to kafka")}
				}
			} else {
			}

		case <-time.After(time.Duration(k.config.MessageTimeoutInMs) * time.Millisecond):
			internalSendMessage.responseChannel <- &messaging.Response{Err: errors2.New("kafka message produce timeout - not sure if this got delivered")}
		}
	}
}
