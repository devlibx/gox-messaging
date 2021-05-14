package kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/devlibx/gox-base"
	errors2 "github.com/devlibx/gox-base/errors"
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
			k.Logger().Info("close the producer internal channel", zap.String("name", k.config.Name))
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
	})
	return nil
}

func (k *kafkaProducerV1) internalSendWork() {
	for msg := range k.messageQueue {
		k.internalSendFunc(msg)
	}
}

func newKafkaProducerV1(cf gox.CrossFunction, config messaging.ProducerConfig) (p messaging.ProducerV1, err error) {
	// Make sure we did get a proper config
	if config.AwsContext == nil || config.AwsContext.GetSession() == nil {
		return nil, errors2.New("Sqs config needs AwsContext which is missing here: name=%s", config.Name)
	}

	// Setup defaults if some inputs are missing
	config.SetupDefaults()

	kp := &kafkaProducerV1{
		config:        config,
		close:         make(chan bool, 1),
		stopDoOnce:    sync.Once{},
		messageQueue:  make(chan *internalSendMessage, config.MaxMessageInBuffer),
		CrossFunction: cf,
	}

	// Make a new kafka producer
	kp.Producer, err = kafka.NewProducer(
		&kafka.ConfigMap{
			"bootstrap.servers": config.Endpoint,
			"acks":              config.Properties["acks"],
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create producer: name="+config.Name)
	}

	// Setup send functions
	if kp.config.Async {
		kp.internalSendFunc = createAsyncInternalSendFuncV1(kp)
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
			internalSendMessage.responseChannel <- &messaging.Response{Err: errors2.Wrap(err, "failed to produce message to kafka")}
			return
		}

		// Use delivery channel to see if we got response
		select {
		case status, _ := <-deliveryChan:
			if ev, ok := status.(*kafka.Message); ok {
				if ev.TopicPartition.Error == nil {
					internalSendMessage.responseChannel <- &messaging.Response{RawPayload: ev}
				} else {
					internalSendMessage.responseChannel <- &messaging.Response{Err: errors2.Wrap(ev.TopicPartition.Error, "failed to produce message to kafka")}
				}
			} else {
			}

		case <-time.After(time.Duration(k.config.MessageTimeoutInMs) * time.Millisecond):
			internalSendMessage.responseChannel <- &messaging.Response{Err: errors2.Wrap(err, "kafka message produce timeout - not sure if this got delivered")}
		}
	}
}
