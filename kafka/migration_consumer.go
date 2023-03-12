package kafka

import (
	"context"
	"github.com/devlibx/gox-base"
	messaging "github.com/devlibx/gox-messaging"
	"go.uber.org/zap"
	"sync"
	"time"
)

type kafkaMigrationConsumerV1 struct {
	primaryConsumer   messaging.Consumer
	migrationConsumer messaging.Consumer
	startOnce         *sync.Once
	stopOnce          *sync.Once
	config            messaging.ConsumerConfig
	logger            *zap.Logger
}

func (k kafkaMigrationConsumerV1) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) (err error) {
	k.startOnce.Do(func() {

		// Start primary consumer
		if e := k.primaryConsumer.Process(ctx, consumeFunction); e != nil {
			err = e
		}

		if e := k.migrationConsumer.Process(ctx, consumeFunction); e != nil {
			err = e
		}

		// If client wants to stop the primary topic consumer after N sec then stop it
		go func() {
			stopAfterSec := k.config.MigrationProperties.IntOrDefault("stop_primary_after_sec", 0)
			if stopAfterSec > 0 {
				k.logger.Warn("Stop primary topic after some time", zap.Int("after_sec", stopAfterSec))
				time.Sleep(time.Duration(stopAfterSec) * time.Second)
				if err := k.primaryConsumer.Stop(); err == nil {
					k.logger.Warn("Stop primary topic after some time: Completed", zap.Int("after_sec", stopAfterSec))
				} else {
					k.logger.Error("Stop primary topic after some time: Failed", zap.Int("after_sec", stopAfterSec), zap.Error(err))
				}
			}
		}()
	})
	return err
}

func (k kafkaMigrationConsumerV1) Stop() (err error) {
	k.stopOnce.Do(func() {
		if k.primaryConsumer != nil {
			err = k.primaryConsumer.Stop()
		}
		if k.migrationConsumer != nil {
			err = k.migrationConsumer.Stop()
		}
	})
	return err
}

func NewMigrationKafkaConsumer(cf gox.CrossFunction, config messaging.ConsumerConfig) (p messaging.Consumer, err error) {
	mkc := &kafkaMigrationConsumerV1{
		startOnce: &sync.Once{},
		stopOnce:  &sync.Once{},
		config:    config,
		logger:    cf.Logger().Named("kafka.consumer.migration").Named(config.Name),
	}

	// Run the primary consumer
	if mkc.primaryConsumer, err = NewKafkaConsumer(cf, config); err != nil {
		return nil, err
	}

	// Run the migration consumer
	configMigration := config
	configMigration.Topic = config.MigrationTopic
	configMigration.Endpoint = config.MigrationEndpoint
	if mkc.migrationConsumer, err = NewKafkaConsumer(cf, configMigration); err != nil {
		return nil, err
	}

	// Setup initial consumer delay if needed
	if c, ok := mkc.migrationConsumer.(*kafkaConsumerV1); ok {
		startMigrationAfterSec := config.MigrationProperties.IntOrDefault("start_migration_after_sec", 0)
		c.initialDelayInConsumerSec = startMigrationAfterSec
	}

	if mkc.config.MigrationProperties == nil {
		mkc.config.MigrationProperties = gox.StringObjectMap{}
	}

	return mkc, nil
}
