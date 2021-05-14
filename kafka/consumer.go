package kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/devlibx/gox-base"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/pkg/errors"
	"time"
)

type kafkaConsumer struct {
	consumers *kafka.Consumer
	gox.CrossFunction
	config *messaging.ConsumerConfig
	close  bool
}

func newKafkaConsumer(cf gox.CrossFunction, config *messaging.ConsumerConfig) (consumer messaging.Consumer, err error) {
	if cf == nil || config == nil {
		return nil, errors.New("input var CrossFunction or ConsumerConfig is nil")
	}

	// Setup default values
	config.SetupDefaults()

	// Setup consumer
	kc := &kafkaConsumer{
		CrossFunction: cf,
		config:        config,
		close:         false,
	}

	kc.consumers, err = kafka.NewConsumer(
		&kafka.ConfigMap{
			"bootstrap.servers": config.Endpoint,
			"group.id":          config.Properties["group.id"],
			"auto.offset.reset": config.Properties["auto.offset.reset"],
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create consumer: name="+config.Name)
	}

	err = kc.consumers.Subscribe(config.Topic, func(consumer *kafka.Consumer, event kafka.Event) error {
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to subscribe consumer: name="+config.Name)
	}
	return kc, nil
}

func (k *kafkaConsumer) Process(ctx context.Context, messagePressedAckChannel chan messaging.Event) (chan messaging.Event, error) {
	readChannel := make(chan messaging.Event, 10)

	go func() {
		for {

			// To stop this goroutine
			select {
			case <-ctx.Done():
				close(readChannel)
				return
			default:
				// NO-OP
			}

			// Consume massages from kafka
			msg, err := k.consumers.ReadMessage(100)
			if err == nil {
				readChannel <- messaging.Event{Key: string(msg.Key), Value: string(msg.Value), RawEvent: msg}
			} else {
				time.Sleep(10 * time.Millisecond)
			}

			if k.close {
				close(readChannel)
				return
			}
		}
	}()

	return readChannel, nil
}

func (k *kafkaConsumer) Stop() error {
	k.close = true
	return nil
}
