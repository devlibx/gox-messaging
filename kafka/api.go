package kafka

import (
	errors2 "errors"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/errors"
	messaging "github.com/devlibx/gox-messaging"
	"go.uber.org/zap"
	"sync"
)

type kafkaMessagingFactory struct {
	producers map[string]messaging.ProducerV1
	consumers map[string]messaging.ConsumerV1
	gox.CrossFunction
	mutex *sync.Mutex
}

func NewKafkaMessagingFactory(cf gox.CrossFunction) messaging.Factory {
	return &kafkaMessagingFactory{
		CrossFunction: cf,
		producers:     map[string]messaging.ProducerV1{},
		consumers:     map[string]messaging.ConsumerV1{},
		mutex:         &sync.Mutex{},
	}
}

func (k *kafkaMessagingFactory) Start(configuration messaging.Configuration) error {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	// Setup producer
	for name, config := range configuration.Producers {
		config.Name = name
		if config.Type == "kafka" {
			producer, err := newKafkaProducerV1(k.CrossFunction, config)
			if err != nil {
				return errors.Wrap(err, "failed to create producer: %s", config.Name)
			}
			k.producers[name] = producer
		}
	}

	// Setup consumers
	for name, config := range configuration.Consumers {
		config.Name = name
		if config.Type == "kafka" {
			consumer, err := newKafkaConsumerV1(k.CrossFunction, config)
			if err != nil {
				return errors.Wrap(err, "failed to create consumer: "+config.Name)
			}
			k.consumers[name] = consumer
		}
	}

	return nil
}

func (k *kafkaMessagingFactory) GetProducer(name string) (messaging.ProducerV1, error) {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	if p, ok := k.producers[name]; ok {
		return p, nil
	} else {
		return nil, messaging.ErrProducerNotFound
	}
}

func (k *kafkaMessagingFactory) GetConsumer(name string) (messaging.ConsumerV1, error) {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	if p, ok := k.consumers[name]; ok {
		return p, nil
	} else {
		return nil, messaging.ErrConsumerNotFound
	}
}

func (k *kafkaMessagingFactory) RegisterProducer(config messaging.ProducerConfig) error {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	if config.Type == "kafka" {
		producer, err := newKafkaProducerV1(k.CrossFunction, config)
		if err != nil {
			return errors.Wrap(err, "failed to create producer: "+config.Name)
		}
		k.producers[config.Name] = producer
	} else {
		return errors2.New("config type must be 'kafka'")
	}
	return nil
}

func (k *kafkaMessagingFactory) RegisterConsumer(config messaging.ConsumerConfig) error {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	if config.Type == "kafka" {
		consumer, err := newKafkaConsumerV1(k.CrossFunction, config)
		if err != nil {
			return errors.Wrap(err, "failed to create consumer: "+config.Name)
		}
		k.consumers[config.Name] = consumer
	} else {
		return errors2.New("config type must be 'kafka'")
	}
	return nil
}

func (k *kafkaMessagingFactory) Stop() error {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	for name, p := range k.producers {
		if err := p.Stop(); err != nil {
			k.Logger().Error("failed to stop producer", zap.String("name", name), zap.Error(err))
		}
	}
	return nil
}
