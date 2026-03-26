package redis

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/devlibx/gox-base/v2/test"
	"github.com/devlibx/gox-base/v2/util"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestRedisMigration(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-migration-%d", time.Now().UnixNano())

	// Producer Config with Migration Enabled via Properties
	config := messaging.ProducerConfig{
		Name:     "test-migration-prod",
		Type:     "redis",
		Endpoint: redisEndpoint,
		Topic:    topic,
		Enabled:  true,
		Properties: map[string]interface{}{
			"migration_enabled":  true,
			"migration_endpoint": redisEndpoint,
			"migration_properties": map[string]interface{}{
				"db": 1, // Use DB 1 for migration environment
			},
		},
	}

	producer, err := NewMigrationSafeRedisProducer(cf, config)
	assert.NoError(t, err)
	defer producer.Stop()

	// Consumer 1: Tracking Primary (DB 0 default)
	consumerConfig1 := messaging.ConsumerConfig{
		Name:        "c1",
		Type:        "redis",
		Endpoint:    redisEndpoint,
		Topic:       topic,
		Enabled:     true,
		Concurrency: 1,
	}
	consumer1, _ := NewRedisConsumer(cf, consumerConfig1)
	defer consumer1.Stop()

	// Consumer 2: Tracking Migration (DB 1)
	consumerConfig2 := messaging.ConsumerConfig{
		Name:        "c2",
		Type:        "redis",
		Endpoint:    redisEndpoint,
		Topic:       topic,
		Enabled:     true,
		Concurrency: 1,
		Properties: map[string]interface{}{
			"db": 1,
		},
	}
	consumer2, _ := NewRedisConsumer(cf, consumerConfig2)
	defer consumer2.Stop()

	var processedCount int32
	consumeFunc := &mockConsumeFunction{
		processFunc: func(message *messaging.Message) error {
			atomic.AddInt32(&processedCount, 1)
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = consumer1.Process(ctx, consumeFunc)
	_ = consumer2.Process(ctx, consumeFunc)

	// Send message
	<-producer.Send(ctx, &messaging.Message{Key: "m1", Payload: "p1", MessageDelayInMs: 0})

	// Wait for processing - expect 2 events (one from each consumer/topic entry)
	assert.Eventually(t, func() bool {
		return atomic.LoadInt32(&processedCount) == 2
	}, 5*time.Second, 100*time.Millisecond)
}
