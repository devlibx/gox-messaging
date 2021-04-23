package kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/harishb2k/gox-base"
	messaging "github.com/harishb2k/gox-messaging"
	"github.com/pkg/errors"
	"sync"
	"time"
)

// Special message to kill this producer

type msg struct {
	key        string
	value      []byte
	resultChan chan error
}

type kafkaProducer struct {
	*kafka.Producer
	gox.CrossFunction
	config           *messaging.ProducerConfig
	messages         chan msg
	close            chan bool
	closed           bool
	mutex            *sync.Mutex
	internalSendFunc func(key string, value []byte, errorCh chan error)
	killMsgKey       string
}

func (k *kafkaProducer) Stop() error {
	k.WithField("name", k.config.Name).Info("start kafka producer close")

	// Lock before we close this producer
	k.mutex.Lock()
	if !k.closed {
		k.close <- true
		close(k.close)
		k.closed = true
		k.messages <- msg{key: k.killMsgKey}
	}
	k.mutex.Unlock()

	k.WithField("name", k.config.Name).Info("close kafka producer completed")
	return nil
}

func newKafkaProducer(cf gox.CrossFunction, config *messaging.ProducerConfig) (p messaging.Producer, err error) {
	if cf == nil || config == nil {
		return nil, errors.New("input var CrossFunction or ProducerConfig is nil")
	}

	// Setup default values
	config.SetupDefaults()

	// Setup producer
	kp := &kafkaProducer{
		CrossFunction: cf,
		config:        config,
		mutex:         &sync.Mutex{},
		close:         make(chan bool, 1),
		messages:      make(chan msg, 1024),
	}

	// Special message to kill this producer
	kp.killMsgKey = uuid.NewString()

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
		kp.internalSendFunc = createAsyncInternalSendFunc(kp)
	} else {
		kp.internalSendFunc = createSyncInternalSendFunc(kp)
	}

	// Start send worker
	go kp.internalSendWork()

	return kp, nil
}

func (k *kafkaProducer) Send(ctx context.Context, key string, data []byte) chan error {
	resultChannel := make(chan error, 1)

	select {
	case _, _ = <-k.close:
		resultChannel <- errors.New("producer already closed")
		close(resultChannel)
		break

	case <-ctx.Done():
		resultChannel <- ctx.Err()
		close(resultChannel)
		break

	default:
		k.messages <- msg{key: key, value: data, resultChan: resultChannel}
	}
	return resultChannel
}

func (k *kafkaProducer) internalSendWork() {
	for msg := range k.messages {
		if msg.key == k.killMsgKey {
			k.WithField("name", k.config.Name).Info("[done] closing send loop for producer")
			break
		}
		k.internalSendFunc(msg.key, msg.value, msg.resultChan)
	}
}

func createAsyncInternalSendFunc(k *kafkaProducer) func(key string, data []byte, errorCh chan error) {
	return func(key string, value []byte, errorCh chan error) {
		errorCh <- k.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &k.config.Topic, Partition: kafka.PartitionAny},
			Value:          value,
			Key:            []byte(key),
		}, nil)
	}
}

func createSyncInternalSendFunc(k *kafkaProducer) func(key string, data []byte, errorCh chan error) {
	return func(key string, value []byte, errorCh chan error) {

		deliveryChan := make(chan kafka.Event, 1)
		if err := k.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &k.config.Topic, Partition: kafka.PartitionAny},
			Value:          value,
			Key:            []byte(key),
		}, deliveryChan); err != nil {
			errorCh <- errors.Wrap(err, "failed to produce message to kafka")
			return
		}

		// Use delivery channel to see if we got response
		select {
		case _ = <-deliveryChan:
			errorCh <- nil

		case <-time.After(time.Duration(k.config.MessageTimeoutInMs) * time.Millisecond):
			k.Error("Timeout for message - " + key)
			errorCh <- errors.New("kafka message produce timeout - not sure if this got delivered")
		}
	}
}
