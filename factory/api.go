package factory

import (
	errors2 "errors"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/errors"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/devlibx/gox-messaging/dummy"
	"github.com/devlibx/gox-messaging/kafka"
	"github.com/devlibx/gox-messaging/sqs"
	"go.uber.org/zap"
	"sync"
)

type messagingFactoryImpl struct {
	producers map[string]messaging.Producer
	consumers map[string]messaging.Consumer
	gox.CrossFunction
	mutex *sync.Mutex
}

func NewMessagingFactory(cf gox.CrossFunction) messaging.Factory {
	return &messagingFactoryImpl{
		CrossFunction: cf,
		producers:     map[string]messaging.Producer{},
		consumers:     map[string]messaging.Consumer{},
		mutex:         &sync.Mutex{},
	}
}

func (k *messagingFactoryImpl) Start(configuration messaging.Configuration) error {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	// Setup producer
	for name, config := range configuration.Producers {
		config.Name = name
		if config.Type == "kafka" {
			producer, err := kafka.NewKafkaProducer(k.CrossFunction, config)
			if err != nil {
				return errors.Wrap(err, "failed to create producer: %s", config.Name)
			}
			k.producers[name] = producer
		} else if config.Type == "sqs" {
			producer, err := sqs.NewSqsProducer(k.CrossFunction, config)
			if err != nil {
				return errors.Wrap(err, "failed to create SQS producer: %s", config.Name)
			}
			k.producers[name] = producer
		} else if config.Type == "dummy" {
			producer := dummy.NewDummyProducer(k.CrossFunction, config)
			k.producers[name] = producer
		}
	}

	// Setup consumers
	for name, config := range configuration.Consumers {
		config.Name = name
		if config.Type == "kafka" {
			consumer, err := kafka.NewKafkaConsumer(k.CrossFunction, config)
			if err != nil {
				return errors.Wrap(err, "failed to create consumer: "+config.Name)
			}
			k.consumers[name] = consumer
		} else if config.Type == "sqs" {
			consumer, err := sqs.NewSqsConsumer(k.CrossFunction, config)
			if err != nil {
				return errors.Wrap(err, "failed to create consumer: "+config.Name)
			}
			k.consumers[name] = consumer
		} else if config.Type == "dummy" {
			consumer := dummy.NewDummyConsumer(k.CrossFunction, config)
			k.consumers[name] = consumer
		}
	}

	return nil
}

func (k *messagingFactoryImpl) GetProducer(name string) (messaging.Producer, error) {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	if p, ok := k.producers[name]; ok {
		return p, nil
	} else {
		return nil, messaging.ErrProducerNotFound
	}
}

func (k *messagingFactoryImpl) GetConsumer(name string) (messaging.Consumer, error) {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	if p, ok := k.consumers[name]; ok {
		return p, nil
	} else {
		return nil, messaging.ErrConsumerNotFound
	}
}

func (k *messagingFactoryImpl) RegisterProducer(config messaging.ProducerConfig) error {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	if config.Type == "kafka" {
		producer, err := kafka.NewKafkaProducer(k.CrossFunction, config)
		if err != nil {
			return errors.Wrap(err, "failed to create producer: "+config.Name)
		}
		k.producers[config.Name] = producer
	} else if config.Type == "sqs" {
		producer, err := sqs.NewSqsProducer(k.CrossFunction, config)
		if err != nil {
			return errors.Wrap(err, "failed to create sqs producer: "+config.Name)
		}
		k.producers[config.Name] = producer
	} else if config.Type == "dummy" {
		producer := dummy.NewDummyProducer(k.CrossFunction, config)
		k.producers[config.Name] = producer
	} else {
		return errors2.New("config type must be 'kafka'")
	}
	return nil
}

func (k *messagingFactoryImpl) RegisterConsumer(config messaging.ConsumerConfig) error {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	if config.Type == "kafka" {
		consumer, err := kafka.NewKafkaConsumer(k.CrossFunction, config)
		if err != nil {
			return errors.Wrap(err, "failed to create consumer: "+config.Name)
		}
		k.consumers[config.Name] = consumer
	} else if config.Type == "sqs" {
		consumer, err := sqs.NewSqsConsumer(k.CrossFunction, config)
		if err != nil {
			return errors.Wrap(err, "failed to create SQS consumer: "+config.Name)
		}
		k.consumers[config.Name] = consumer
	} else if config.Type == "dummy" {
		consumer := dummy.NewDummyConsumer(k.CrossFunction, config)
		k.consumers[config.Name] = consumer
	} else {
		return errors2.New("config type must be 'kafka'")
	}
	return nil
}

func (k *messagingFactoryImpl) Stop() error {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	for name, p := range k.producers {
		if err := p.Stop(); err != nil {
			k.Logger().Error("failed to stop producer", zap.String("name", name), zap.Error(err))
		} else {
			k.Logger().Debug("stopped producer", zap.String("name", name), zap.Any("producer", p))
		}
	}

	for name, c := range k.consumers {
		if err := c.Stop(); err != nil {
			k.Logger().Error("failed to stop consumer", zap.String("name", name), zap.Error(err))
		} else {
			k.Logger().Debug("stopped consumer", zap.String("name", name), zap.Any("consumer", c))
		}
	}
	return nil
}
