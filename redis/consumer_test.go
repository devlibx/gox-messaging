package redis

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/devlibx/gox-base/v2/test"
	"github.com/devlibx/gox-base/v2/util"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type mockConsumeFunction struct {
	processFunc func(message *messaging.Message) error
}

func (m *mockConsumeFunction) Process(message *messaging.Message) error {
	return m.processFunc(message)
}

func (m *mockConsumeFunction) ErrorInProcessing(message *messaging.Message, err error) {}

func TestRedisConsumerVisibilityAndRetry(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-consumer-%d", time.Now().UnixNano())

	// 1. Setup Producer
	producerConfig := messaging.ProducerConfig{
		Name:        "test-prod",
		Type:        "redis",
		Endpoint:    redisEndpoint,
		Topic:       topic,
		Enabled:     true,
		Properties:  map[string]interface{}{"max_attempts": 3},
		Concurrency: 1,
	}
	producer, err := NewRedisProducer(cf, producerConfig)
	if err != nil {
		t.Skip("Redis not available")
		return
	}
	defer producer.Stop()

	// 2. Setup Consumer with short visibility timeout
	consumerConfig := messaging.ConsumerConfig{
		Name:     "test-cons",
		Type:     "redis",
		Endpoint: redisEndpoint,
		Topic:    topic,
		Enabled:  true,
		Properties: map[string]interface{}{
			"visibility_timeout_ms":     100,  // 0.1 second visibility
			"max_visibility_timeout_ms": 1000, // max 1 second
			"backoff_multiplier":        2.0,  // 0.1 -> 0.2 -> 0.4 -> 0.8 -> 1.0
			"batch_size":                10,
		},
		Concurrency: 1,
	}
	consumer, err := NewRedisConsumer(cf, consumerConfig)
	assert.NoError(t, err)
	defer consumer.Stop()

	var processedCount int32
	var failedCount int32
	
	// Consumer function that fails the first time for a specific message
	consumeFunc := &mockConsumeFunction{
		processFunc: func(message *messaging.Message) error {
			if message.Key == "fail-me" {
				atomic.AddInt32(&failedCount, 1)
				return fmt.Errorf("intentional failure")
			}
			atomic.AddInt32(&processedCount, 1)
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = consumer.Process(ctx, consumeFunc)
	assert.NoError(t, err)

	// Send one successful message
	<-producer.Send(ctx, &messaging.Message{Key: "ok-1", Payload: "data-1"})

	// Send one message that will fail and should be retried
	<-producer.Send(ctx, &messaging.Message{Key: "fail-me", Payload: "data-fail"})

	// Wait for processing
	time.Sleep(15 * time.Second)

	// "ok-1" should be processed once
	assert.Equal(t, int32(1), atomic.LoadInt32(&processedCount))
	
	// "fail-me" should be attempted 3 times (initial + 2 retries) and then dropped
	assert.GreaterOrEqual(t, atomic.LoadInt32(&failedCount), int32(3))

	// Verify Redis is empty
	p := producer.(*redisProducer)
	keys, _ := p.redisClient.Keys(ctx, "job:{"+topic+"}:*").Result()
	assert.Equal(t, 0, len(keys))
}

func TestRedisConsumerBatch(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-batch-%d", time.Now().UnixNano())

	producerConfig := messaging.ProducerConfig{
		Name:        "test-prod-batch",
		Type:        "redis",
		Endpoint:    redisEndpoint,
		Topic:       topic,
		Enabled:     true,
		Concurrency: 5,
	}
	producer, _ := NewRedisProducer(cf, producerConfig)
	defer producer.Stop()

	consumerConfig := messaging.ConsumerConfig{
		Name:        "test-cons-batch",
		Type:        "redis",
		Endpoint:    redisEndpoint,
		Topic:       topic,
		Enabled:     true,
		Concurrency: 5,
		Properties: map[string]interface{}{
			"batch_size":            50,
			"visibility_timeout_ms": 5000,
		},
	}
	consumer, _ := NewRedisConsumer(cf, consumerConfig)
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

	// Send 100 messages
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-producer.Send(ctx, &messaging.Message{Key: fmt.Sprintf("m-%d", id), Payload: "data"})
		}(i)
	}
	wg.Wait()

	// Wait for processing
	assert.Eventually(t, func() bool {
		return atomic.LoadInt32(&processedCount) == 100
	}, 10*time.Second, 500*time.Millisecond)

	// Verify Redis is empty
	p := producer.(*redisProducer)
	keys, _ := p.redisClient.Keys(ctx, "job:{"+topic+"}:*").Result()
	assert.Equal(t, 0, len(keys))
}
