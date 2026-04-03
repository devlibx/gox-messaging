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
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestRedisMigration(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-migration-%d", time.Now().UnixNano())
	serviceName := "test-" + uuid.NewString()
// Producer Config with Migration Enabled via Properties
config := messaging.ProducerConfig{
	Name:                 "test-migration-prod",
	Type:                 "redis",
	Endpoint:             redisEndpoint,
	Topic:                topic,
	Enabled:              true,
	MandatoryServiceName: serviceName,
	Properties: map[string]interface{}{
		"migration_enabled":  true,
		"migration_endpoint": redisEndpoint,
		"migration_properties": map[string]interface{}{
			"db": 1, // Use DB 1 for migration environment
		},
	},
}

producer, err := NewMigrationSafeRedisProducer(cf, config)
if err != nil {
	t.Skip("Redis not available")
	return
}
defer producer.Stop()

// Composite Consumer with Migration Enabled via Properties
consumerConfig := messaging.ConsumerConfig{
	Name:                 "composite-consumer",
	Type:                 "redis",
	Endpoint:             redisEndpoint,
	Topic:                topic,
	Enabled:              true,
	Concurrency:          2,
	MandatoryServiceName: serviceName,
	Properties: map[string]interface{}{
		"migration_enabled":  true,
		"migration_endpoint": redisEndpoint,
		"migration_db":       1,
	},
}
consumer, err := NewMigrationSafeRedisConsumer(cf, consumerConfig)
assert.NoError(t, err)
defer consumer.Stop()

	var processedCount int32
	consumeFunc := &mockConsumeFunction{
		processFunc: func(message *messaging.Message) error {
			atomic.AddInt32(&processedCount, 1)
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = consumer.Process(ctx, consumeFunc)

	// Send message
	<-producer.Send(ctx, &messaging.Message{Key: "m1", Payload: "p1", MessageDelayInMs: 0})

	// Wait for processing - expect 2 events (one from each underlying consumer)
	assert.Eventually(t, func() bool {
		return atomic.LoadInt32(&processedCount) == 2
	}, 10*time.Second, 100*time.Millisecond)
}
