package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/harishb2k/gox-base"
	messaging "github.com/harishb2k/gox-messaging"
	"github.com/pkg/errors"
	"time"
)

type kafkaConsumer struct {
	consumers []*kafka.Consumer
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

	kc.consumers = make([]*kafka.Consumer, kc.config.Concurrency)
	for i := 0; i < kc.config.Concurrency; i++ {
		kc.consumers[i], err = kafka.NewConsumer(
			&kafka.ConfigMap{
				"bootstrap.servers": config.Endpoint,
				"group.id":          config.Properties["group.id"],
				"auto.offset.reset": config.Properties["auto.offset.reset"],
			},
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create consumer: name="+config.Name)
		}

		err = kc.consumers[i].Subscribe(kc.config.Topic, func(consumer *kafka.Consumer, event kafka.Event) error {
			return nil
		})
		if err != nil {
			return nil, errors.Wrap(err, "failed to subscribe consumer: name="+config.Name)
		}
	}

	return kc, nil
}

func (k *kafkaConsumer) Start(consumerFunc messaging.ConsumerFunc) error {
	for i := 0; i < k.config.Concurrency; i++ {
		go func(id int) {
			k.consumer(k.consumers[id], consumerFunc)()
		}(i)
	}
	return nil
}

func (k *kafkaConsumer) consumer(consumer *kafka.Consumer, consumerFunc messaging.ConsumerFunc) func() {

	type internalMsg struct {
		kafkaMsg *kafka.Message
		err      error
	}
	messages := make(chan internalMsg, 1024)

	return func() {

		go func() {
			for {

				// Consume massages from kafka
				msg, err := consumer.ReadMessage(100)
				if err == nil {
					messages <- internalMsg{kafkaMsg: msg, err: nil}
				} else {
					k.WithError(err).Info("[done] error")
					time.Sleep(1 * time.Second)
				}

				// Close this channel
				if k.close {
					close(messages)
				}
			}
		}()

		// Consumer loop
		for {
			msg, isOpen := <-messages
			if isOpen {
				_ = consumerFunc(&messaging.Message{Key: string(msg.kafkaMsg.Key), Data: msg.kafkaMsg.Value})
			} else {
				k.WithField("name", k.config.Name).Info("[done] closing message consume loop")
				break
			}
		}
	}
}

func (k *kafkaConsumer) Stop() error {
	k.close = true
	return nil
}
