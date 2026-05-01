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
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestRedisPriorityConsumer(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-prio-%d", time.Now().UnixNano())
	serviceName := "test-" + uuid.NewString()

	// 1. Setup Producer
	producer, err := NewRedisProducer(cf, messaging.ProducerConfig{
		Name: "p", Type: "redis", Topic: topic, Enabled: true, Endpoint: redisEndpoint,
		MandatoryServiceName: serviceName,
		Properties: map[string]interface{}{
			"priority_enabled": true,
		},
	})
	if err != nil {
		t.Skip("Redis not available")
		return
	}
	defer producer.Stop()

	// 2. Setup Priority Consumer
	consumer, err := NewRedisPriorityConsumer(cf, messaging.ConsumerConfig{
		Name: "c", Type: "redis", Topic: topic, Enabled: true, Endpoint: redisEndpoint,
		MandatoryServiceName: serviceName,
		Concurrency: 1, // Use 1 to ensure sequential processing for test
		Properties: map[string]interface{}{
			"priority_enabled": true,
		},
	})
	assert.NoError(t, err)
	defer consumer.Stop()

	var processedKeys []string
	consumeFunc := &mockConsumeFunction{
		processFunc: func(message *messaging.Message) error {
			processedKeys = append(processedKeys, message.Key)
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = consumer.Process(ctx, consumeFunc)

	// Send messages with different priorities and 2s delay
	// We send them in "wrong" order (Low priority first)
	<-producer.Send(ctx, &messaging.Message{Key: "prio-2", Payload: "p2", Priority: 2, MessageDelayInMs: 2000})
	<-producer.Send(ctx, &messaging.Message{Key: "prio-1", Payload: "p1", Priority: 1, MessageDelayInMs: 2000})
	<-producer.Send(ctx, &messaging.Message{Key: "prio-0", Payload: "p0", Priority: 0, MessageDelayInMs: 2000})

	// Wait for processing
	time.Sleep(5 * time.Second)

	// Verify order: prio-0 should be first, then prio-1, then prio-2
	assert.Equal(t, 3, len(processedKeys))
	if len(processedKeys) == 3 {
		assert.Equal(t, "prio-0", processedKeys[0])
		assert.Equal(t, "prio-1", processedKeys[1])
		assert.Equal(t, "prio-2", processedKeys[2])
	}
}

func TestRedisPriorityConsumerRetryMaintainsPriority(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-prio-retry-%d", time.Now().UnixNano())
	serviceName := "test-" + uuid.NewString()

	producer, err := NewRedisProducer(cf, messaging.ProducerConfig{
		Name: "p", Type: "redis", Topic: topic, Enabled: true, Endpoint: redisEndpoint,
		MandatoryServiceName: serviceName,
		Properties: map[string]interface{}{
			"visibility_timeout_ms": 100,
			"priority_enabled":      true,
		},
	})
	if err != nil {
		t.Skip("Redis not available")
		return
	}
	defer producer.Stop()

	consumer, err := NewRedisPriorityConsumer(cf, messaging.ConsumerConfig{
		Name: "c", Type: "redis", Topic: topic, Enabled: true, Endpoint: redisEndpoint,
		MandatoryServiceName: serviceName,
		Concurrency: 1,
		Properties: map[string]interface{}{
			"visibility_timeout_ms": 100,
			"backoff_multiplier":    1.0,
			"priority_enabled":      true,
		},
	})
	assert.NoError(t, err)
	if consumer == nil {
		return
	}
	defer consumer.Stop()

	var processedKeys []string
	var mu sync.Mutex
	var retryCount int32
	consumeFunc := &mockConsumeFunction{
		processFunc: func(message *messaging.Message) error {
			mu.Lock()
			processedKeys = append(processedKeys, message.Key)
			mu.Unlock()
			if message.Key == "prio-retry" && atomic.LoadInt32(&retryCount) == 0 {
				atomic.AddInt32(&retryCount, 1)
				return fmt.Errorf("fail once")
			}
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = consumer.Process(ctx, consumeFunc)

	// 1. Send a high priority message that will fail and be retried
	<-producer.Send(ctx, &messaging.Message{Key: "prio-retry", Payload: "pr", Priority: 1})

	// 2. Send a lower priority message
	time.Sleep(1 * time.Second)
	<-producer.Send(ctx, &messaging.Message{Key: "prio-2", Payload: "p2", Priority: 2})

	// Wait for processing
	time.Sleep(5 * time.Second)

	// Verify order
	mu.Lock()
	defer mu.Unlock()
	fmt.Println("Processed keys:", processedKeys)
	assert.GreaterOrEqual(t, len(processedKeys), 3) // prio-retry (fail), then prio-retry (ok), then prio-2 (ok)
	if len(processedKeys) >= 3 {
		assert.Equal(t, "prio-retry", processedKeys[0])
		assert.Equal(t, "prio-retry", processedKeys[1])
		assert.Equal(t, "prio-2", processedKeys[2])
	}
}

func TestRedisPriorityConsumerRequeueable(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-prio-requeue-%d", time.Now().UnixNano())
	serviceName := "test-" + uuid.NewString()

	producer, _ := NewRedisProducer(cf, messaging.ProducerConfig{
		Name: "p", Type: "redis", Topic: topic, Enabled: true, Endpoint: redisEndpoint, MandatoryServiceName: serviceName,
		Properties: map[string]interface{}{"max_attempts": 2, "priority_enabled": true},
	})
	defer producer.Stop()

	consumer, _ := NewRedisPriorityConsumer(cf, messaging.ConsumerConfig{
		Name: "c", Type: "redis", Topic: topic, Enabled: true, Endpoint: redisEndpoint, MandatoryServiceName: serviceName,
		Properties: map[string]interface{}{"priority_enabled": true},
	})
	defer consumer.Stop()

	var processedCount int32
	consumeFunc := &mockConsumeFunction{
		processFunc: func(message *messaging.Message) error {
			count := atomic.AddInt32(&processedCount, 1)
			if count <= 5 {
				return &requeueableError{delay: 10}
			}
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = consumer.Process(ctx, consumeFunc)

	// Send message
	<-producer.Send(ctx, &messaging.Message{Key: "prio-requeue-test", Payload: "data", Priority: 1})

	// It should be processed 6 times
	time.Sleep(2 * time.Second)
	assert.Equal(t, int32(6), atomic.LoadInt32(&processedCount))
}

func TestRedisPriorityConsumerThrottlable(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-prio-throttle-%d", time.Now().UnixNano())
	serviceName := "test-" + uuid.NewString()

	consumer, _ := NewRedisPriorityConsumer(cf, messaging.ConsumerConfig{
		Name: "c", Type: "redis", Topic: topic, Enabled: true, Endpoint: redisEndpoint, MandatoryServiceName: serviceName,
		Properties: map[string]interface{}{"priority_enabled": true},
	})
	defer consumer.Stop()

	producer, _ := NewRedisProducer(cf, messaging.ProducerConfig{
		Name: "p", Type: "redis", Topic: topic, Enabled: true, Endpoint: redisEndpoint, MandatoryServiceName: serviceName,
		Properties: map[string]interface{}{"priority_enabled": true},
	})
	defer producer.Stop()

	var processedCount int32
	consumeFunc := &mockConsumeFunction{
		processFunc: func(message *messaging.Message) error {
			atomic.AddInt32(&processedCount, 1)
			return &throttlableError{sleepMs: 1000} // Sleep for 1 second
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = consumer.Process(ctx, consumeFunc)

	// Send 2 messages
	<-producer.Send(ctx, &messaging.Message{Key: "t1", Payload: "d1", Priority: 1})
	<-producer.Send(ctx, &messaging.Message{Key: "t2", Payload: "d2", Priority: 1})

	time.Sleep(500 * time.Millisecond)
	assert.LessOrEqual(t, atomic.LoadInt32(&processedCount), int32(1))

	time.Sleep(2 * time.Second)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&processedCount), int32(2))
}

func BenchmarkRedisPriorityConsumerThroughput(b *testing.B) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(b, zap.ErrorLevel)
	topic := fmt.Sprintf("bench-prio-%d", time.Now().UnixNano())
	serviceName := "test-" + uuid.NewString()

	// 1. Setup Producer with Priority Enabled
	producer, _ := NewRedisProducer(cf, messaging.ProducerConfig{
		Name:     "p",
		Type:     "redis",
		Topic:    topic,
		Enabled:  true,
		Endpoint: redisEndpoint,
		MandatoryServiceName: serviceName,
		Properties: map[string]interface{}{
			"priority_enabled": true,
		},
	})

	// 2. Setup Priority Consumer
	consumer, _ := NewRedisPriorityConsumer(cf, messaging.ConsumerConfig{
		Name:        "c",
		Type:        "redis",
		Topic:       topic,
		Enabled:     true,
		Endpoint:    redisEndpoint,
		MandatoryServiceName: serviceName,
		Concurrency: 20,
		Properties: map[string]interface{}{
			"batch_size":       100,
			"priority_enabled": true,
		},
	})

	defer func() {
		p := producer.(*redisProducer)
		ctx := context.Background()
		// Clean up isolated priority queues
		p.redisClient.Del(ctx, p.getQueueKey("scheduled_jobs"), p.getQueueKey("runnable_jobs"), p.getQueueKey("visibility"))
		producer.Stop()
		consumer.Stop()
	}()

	totalMessages := 10000
	consumerWg := &sync.WaitGroup{}
	consumerWg.Add(totalMessages)

	var processedCount int64
	
	consumeFunc := &mockConsumeFunction{
		processFunc: func(message *messaging.Message) error {
			atomic.AddInt64(&processedCount, 1)
			consumerWg.Done()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Produce messages with mixed priorities
	for i := 0; i < totalMessages; i++ {
		prio := i % 3 // 0, 1, 2
		<-producer.Send(ctx, &messaging.Message{
			Payload:  "bench payload",
			Priority: prio,
		})
	}

	start := time.Now()
	_ = consumer.Process(ctx, consumeFunc)
	
	done := make(chan struct{})
	go func() {
		consumerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		elapsed := time.Since(start)
		b.Logf("Processed %d prioritized messages in %v (%.2f msg/sec)", 
			totalMessages, elapsed, float64(totalMessages)/elapsed.Seconds())
	case <-time.After(30 * time.Second):
		b.Errorf("Benchmark timed out. Processed: %d", atomic.LoadInt64(&processedCount))
	}
}
