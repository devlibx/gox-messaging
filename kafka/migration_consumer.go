package kafka

import (
	"context"
	"github.com/devlibx/gox-base"
	messaging "github.com/devlibx/gox-messaging"
	"sync"
)

type kafkaMigrationConsumerV1 struct {
	primaryConsumer   messaging.Consumer
	migrationConsumer messaging.Consumer
	startOnce         *sync.Once
	stopOnce          *sync.Once
}

func (k kafkaMigrationConsumerV1) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) (err error) {
	k.startOnce.Do(func() {
		if e := k.primaryConsumer.Process(ctx, consumeFunction); e != nil {
			err = e
		}
		if e := k.migrationConsumer.Process(ctx, consumeFunction); e != nil {
			err = e
		}
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

	return mkc, nil
}
