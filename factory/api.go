package factory

import (
	errors2 "errors"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/devlibx/gox-messaging/v2/dummy"
	"github.com/devlibx/gox-messaging/v2/kafka"
	"github.com/devlibx/gox-messaging/v2/pubsub"
	internalRedis "github.com/devlibx/gox-messaging/v2/redis"
	"github.com/devlibx/gox-messaging/v2/sqs"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"sync"
)

type messagingFactoryImpl struct {
	producers    map[string]messaging.Producer
	consumers    map[string]messaging.Consumer
	redisClients map[string]redis.UniversalClient
	gox.CrossFunction
	mutex   *sync.Mutex
	started bool
}

func NewMessagingFactory(cf gox.CrossFunction) messaging.Factory {
	return &messagingFactoryImpl{
		CrossFunction: cf,
		producers:     map[string]messaging.Producer{},
		consumers:     map[string]messaging.Consumer{},
		redisClients:  map[string]redis.UniversalClient{},
		mutex:         &sync.Mutex{},
	}
}

func (k *messagingFactoryImpl) MarkStart() {
	k.mutex.Lock()
	defer k.mutex.Unlock()
	k.started = true
}

func (k *messagingFactoryImpl) setupRedisClientForProducer(config *messaging.ProducerConfig) {
	if config.Type == "redis" {
		key := internalRedis.GetRedisClientKey(config.Endpoint, config.Properties)
		if client, ok := k.redisClients[key]; ok {
			config.RedisClient = client
		} else {
			client, err := internalRedis.CreateRedisUniversalClient(config.Endpoint, config.Properties)
			if err == nil {
				k.redisClients[key] = client
				config.RedisClient = client
			}
		}
	}
}

func (k *messagingFactoryImpl) setupRedisClientForConsumer(config *messaging.ConsumerConfig) {
	if config.Type == "redis" {
		key := internalRedis.GetRedisClientKey(config.Endpoint, config.Properties)
		if client, ok := k.redisClients[key]; ok {
			config.RedisClient = client
		} else {
			client, err := internalRedis.CreateRedisUniversalClient(config.Endpoint, config.Properties)
			if err == nil {
				k.redisClients[key] = client
				config.RedisClient = client
			}
		}
	}
}

func (k *messagingFactoryImpl) setupResources(configuration messaging.Configuration) {
	if k.redisClients == nil {
		k.redisClients = make(map[string]redis.UniversalClient)
	}

	for name, config := range configuration.Producers {
		k.setupRedisClientForProducer(&config)
		configuration.Producers[name] = config
	}

	for name, config := range configuration.Consumers {
		k.setupRedisClientForConsumer(&config)
		configuration.Consumers[name] = config
	}
}

func (k *messagingFactoryImpl) Start(configuration messaging.Configuration) error {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	// Setup redis clients
	k.setupResources(configuration)

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
		} else if config.Type == "pubsub" {
			producer, err := pubsub.NewPubSubProducer(k.CrossFunction.Logger(), config)
			if err != nil {
				return errors.Wrap(err, "failed to create pubsub producer: %s", config.Name)
			}
			k.producers[name] = producer
		} else if config.Type == "redis" {
			migrationEnabled, _ := config.Properties["migration_enabled"].(bool)
			var producer messaging.Producer
			var err error
			if migrationEnabled {
				producer, err = internalRedis.NewMigrationSafeRedisProducer(k.CrossFunction, config)
				if err != nil {
					return errors.Wrap(err, "failed to create redis producer with migration: "+config.Name)
				}
			} else {
				producer, err = internalRedis.NewRedisProducer(k.CrossFunction, config)
				if err != nil {
					return errors.Wrap(err, "failed to create redis producer: %s", config.Name)
				}
			}
			k.producers[name] = producer
		}
	}

	// Setup consumers
	for name, config := range configuration.Consumers {
		config.Name = name
		if config.Type == "kafka" {
			// Check if this is a migration consumer or not
			migrationEnabled, err := config.IsMigrationEnabled()
			if err != nil {
				return errors.Wrap(err, "failed to check if enabled migration or not: "+config.Name)
			}

			// Based on consumer migration - create different type of consumers
			var consumer messaging.Consumer
			if migrationEnabled {
				consumer, err = kafka.NewMigrationKafkaConsumer(k.CrossFunction, config)
				if err != nil {
					return errors.Wrap(err, "failed to create consumer with migration: "+config.Name)
				}
			} else {
				consumer, err = kafka.NewKafkaConsumer(k.CrossFunction, config)
				if err != nil {
					return errors.Wrap(err, "failed to create consumer: "+config.Name)
				}
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
		} else if config.Type == "pubsub" {
			consumer, err := pubsub.NewPubSubConsumer(k.CrossFunction.Logger(), config)
			if err != nil {
				return errors.Wrap(err, "failed to create pubsub consumer: %s", config.Name)
			}
			k.consumers[name] = consumer
		} else if config.Type == "redis" {
			migrationEnabled, _ := config.Properties["migration_enabled"].(bool)
			priorityEnabled, _ := config.Properties["priority_enabled"].(bool)
			var consumer messaging.Consumer
			var err error
			if migrationEnabled {
				consumer, err = internalRedis.NewMigrationSafeRedisConsumer(k.CrossFunction, config)
				if err != nil {
					return errors.Wrap(err, "failed to create redis consumer with migration: "+config.Name)
				}
			} else if priorityEnabled {
				consumer, err = internalRedis.NewRedisPriorityConsumer(k.CrossFunction, config)
				if err != nil {
					return errors.Wrap(err, "failed to create redis consumer with priority: "+config.Name)
				}
			} else {
				consumer, err = internalRedis.NewRedisConsumer(k.CrossFunction, config)
				if err != nil {
					return errors.Wrap(err, "failed to create redis consumer: "+config.Name)
				}
			}
			k.consumers[name] = consumer
		}
	}

	k.started = true

	return nil
}

func (k *messagingFactoryImpl) GetProducer(name string) (messaging.Producer, error) {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	if !k.started {
		return nil, errors.New("Please call Start() on message factory before calling GetProducer")
	}

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

	if !k.started {
		return nil, errors.New("Please call Start() on message factory before calling GetConsumer")
	}

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

	k.setupRedisClientForProducer(&config)

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
	} else if config.Type == "pubsub" {
		producer, err := pubsub.NewPubSubProducer(k.CrossFunction.Logger(), config)
		if err != nil {
			return errors.Wrap(err, "failed to create pubsub producer: "+config.Name)
		}
		k.producers[config.Name] = producer
	} else if config.Type == "redis" {
		migrationEnabled, _ := config.Properties["migration_enabled"].(bool)
		var producer messaging.Producer
		var err error
		if migrationEnabled {
			producer, err = internalRedis.NewMigrationSafeRedisProducer(k.CrossFunction, config)
			if err != nil {
				return errors.Wrap(err, "failed to create redis producer with migration: "+config.Name)
			}
		} else {
			producer, err = internalRedis.NewRedisProducer(k.CrossFunction, config)
			if err != nil {
				return errors.Wrap(err, "failed to create redis producer: %s", config.Name)
			}
		}
		k.producers[config.Name] = producer
	} else {
		return errors2.New("config type must be 'kafka' or 'sqs' or 'dummy' or 'pubsub' or 'redis'")
	}
	return nil
}

func (k *messagingFactoryImpl) RegisterConsumer(config messaging.ConsumerConfig) error {
	// Take a lock before yoy do anything
	k.mutex.Lock()
	defer k.mutex.Unlock()

	k.setupRedisClientForConsumer(&config)

	if config.Type == "kafka" {

		// Check if this is a migration consumer or not
		migrationEnabled, err := config.IsMigrationEnabled()
		if err != nil {
			return errors.Wrap(err, "failed to check if enabled migration or not: "+config.Name)
		}

		// Based on consumer migration - create different type of consumers
		var consumer messaging.Consumer
		if migrationEnabled {
			consumer, err = kafka.NewMigrationKafkaConsumer(k.CrossFunction, config)
			if err != nil {
				return errors.Wrap(err, "failed to create consumer with migration: "+config.Name)
			}
		} else {
			consumer, err = kafka.NewKafkaConsumer(k.CrossFunction, config)
			if err != nil {
				return errors.Wrap(err, "failed to create consumer: "+config.Name)
			}
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
	} else if config.Type == "pubsub" {
		consumer, err := pubsub.NewPubSubConsumer(k.CrossFunction.Logger(), config)
		if err != nil {
			return errors.Wrap(err, "failed to create pubsub consumer: "+config.Name)
		}
		k.consumers[config.Name] = consumer
	} else if config.Type == "redis" {
		migrationEnabled, _ := config.Properties["migration_enabled"].(bool)
		priorityEnabled, _ := config.Properties["priority_enabled"].(bool)
		var consumer messaging.Consumer
		var err error
		if migrationEnabled {
			consumer, err = internalRedis.NewMigrationSafeRedisConsumer(k.CrossFunction, config)
			if err != nil {
				return errors.Wrap(err, "failed to create redis consumer with migration: "+config.Name)
			}
		} else if priorityEnabled {
			consumer, err = internalRedis.NewRedisPriorityConsumer(k.CrossFunction, config)
			if err != nil {
				return errors.Wrap(err, "failed to create redis consumer with priority: "+config.Name)
			}
		} else {
			consumer, err = internalRedis.NewRedisConsumer(k.CrossFunction, config)
			if err != nil {
				return errors.Wrap(err, "failed to create redis consumer: "+config.Name)
			}
		}
		k.consumers[config.Name] = consumer
	} else {
		return errors2.New("config type must be 'kafka' or 'sqs' or 'dummy' or 'pubsub' or 'redis'")
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

	// If we have consumers then we need to start closing them in parallel
	// Consumers like kafka may take upto 10 sec to close. If we have 10 consumers then it will take 100 sec. Instead
	// we close them in parallel so the total time will be ~10ses
	if (len(k.consumers)) > 0 {
		wg := sync.WaitGroup{}
		for name, c := range k.consumers {
			wg.Add(1)
			go func(name string, c messaging.Consumer) {
				defer wg.Done()
				if err := c.Stop(); err != nil {
					k.Logger().Error("failed to stop consumer", zap.String("name", name), zap.Error(err))
				} else {
					k.Logger().Debug("stopped consumer", zap.String("name", name), zap.Any("consumer", c))
				}
			}(name, c)
		}
		wg.Wait()
	}

	// Close redis clients
	for _, client := range k.redisClients {
		_ = client.Close()
	}

	return nil
}
