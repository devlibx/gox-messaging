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

// TestRedisConsumerGlobalRateLimiterPerMessage verifies that the global rate
// limiter hook is invoked exactly once per successfully consumed message (not
// once per batch fetch). This pins the per-message contract: the hook lives
// inside the inner message loop, next to c.ratelimit.Take().
func TestRedisConsumerGlobalRateLimiterPerMessage(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-grl-%d", time.Now().UnixNano())
	serviceName := "test-" + uuid.NewString()
	consumerName := "grl-cons"

	producer, err := NewRedisProducer(cf, messaging.ProducerConfig{
		Name: "p", Type: "redis", Topic: topic, Enabled: true,
		Endpoint: redisEndpoint, MandatoryServiceName: serviceName,
	})
	if err != nil {
		t.Skip("Redis not available")
		return
	}

	consumer, err := NewRedisConsumer(cf, messaging.ConsumerConfig{
		Name: consumerName, Type: "redis", Topic: topic, Enabled: true,
		Endpoint: redisEndpoint, MandatoryServiceName: serviceName,
		// Large batch on purpose: if the hook were called per-fetch instead of
		// per-message, one fetch would grab all messages and the count would be
		// far less than the number of messages sent.
		Properties: map[string]interface{}{"batch_size": 100},
	})
	assert.NoError(t, err)

	// Override the global rate limiter with a per-consumer-name counter.
	var grlCalls int64
	var sawProducerFlag int32 // 1 if we ever saw producerOrConsumer==true
	originalGRL := GlobalRateLimiter
	GlobalRateLimiter = func(producerOrConsumer bool, name string) error {
		if name != consumerName {
			return nil // ignore any unrelated consumers
		}
		if producerOrConsumer {
			atomic.StoreInt32(&sawProducerFlag, 1)
		}
		atomic.AddInt64(&grlCalls, 1)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Teardown in a fixed order so the worker goroutine has stopped reading the
	// package-level GlobalRateLimiter var before we restore it (Stop() does not
	// wait for workerLoop to exit).
	defer func() {
		cancel()
		_ = consumer.Stop()
		_ = producer.Stop()
		time.Sleep(300 * time.Millisecond)
		GlobalRateLimiter = originalGRL
	}()

	var processedCount int64
	consumeFunc := &mockConsumeFunction{
		processFunc: func(message *messaging.Message) error {
			atomic.AddInt64(&processedCount, 1)
			return nil
		},
	}

	err = consumer.Process(ctx, consumeFunc)
	assert.NoError(t, err)

	const n = 5
	for i := 0; i < n; i++ {
		<-producer.Send(ctx, &messaging.Message{Key: fmt.Sprintf("m-%d", i), Payload: "data"})
	}

	// Wait for all messages to be consumed.
	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&processedCount) == int64(n)
	}, 8*time.Second, 100*time.Millisecond, "expected all %d messages to be processed", n)

	// The hook must have fired exactly once per message.
	assert.Equal(t, int64(n), atomic.LoadInt64(&grlCalls),
		"global rate limiter should be called once per message, not once per batch")
	// All call sites are consumers, so the flag is always false.
	assert.Equal(t, int32(0), atomic.LoadInt32(&sawProducerFlag),
		"producerOrConsumer should be false for consumer call sites")
}

// TestRedisPriorityConsumerGlobalRateLimiterPerMessage is the same contract
// check for the priority consumer's worker loop.
func TestRedisPriorityConsumerGlobalRateLimiterPerMessage(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-grl-prio-%d", time.Now().UnixNano())
	serviceName := "test-" + uuid.NewString()
	consumerName := "grl-prio-cons"

	producer, err := NewRedisProducer(cf, messaging.ProducerConfig{
		Name: "p", Type: "redis", Topic: topic, Enabled: true,
		Endpoint: redisEndpoint, MandatoryServiceName: serviceName,
		Properties: map[string]interface{}{"priority_enabled": true},
	})
	if err != nil {
		t.Skip("Redis not available")
		return
	}

	consumer, err := NewRedisPriorityConsumer(cf, messaging.ConsumerConfig{
		Name: consumerName, Type: "redis", Topic: topic, Enabled: true,
		Endpoint: redisEndpoint, MandatoryServiceName: serviceName,
		Properties: map[string]interface{}{"priority_enabled": true, "batch_size": 100},
	})
	assert.NoError(t, err)

	var grlCalls int64
	originalGRL := GlobalRateLimiter
	GlobalRateLimiter = func(producerOrConsumer bool, name string) error {
		if name == consumerName {
			atomic.AddInt64(&grlCalls, 1)
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		_ = consumer.Stop()
		_ = producer.Stop()
		time.Sleep(300 * time.Millisecond)
		GlobalRateLimiter = originalGRL
	}()

	var processedCount int64
	consumeFunc := &mockConsumeFunction{
		processFunc: func(message *messaging.Message) error {
			atomic.AddInt64(&processedCount, 1)
			return nil
		},
	}

	err = consumer.Process(ctx, consumeFunc)
	assert.NoError(t, err)

	const n = 5
	for i := 0; i < n; i++ {
		<-producer.Send(ctx, &messaging.Message{Key: fmt.Sprintf("m-%d", i), Payload: "data", Priority: i % 3})
	}

	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&processedCount) == int64(n)
	}, 8*time.Second, 100*time.Millisecond, "expected all %d messages to be processed", n)

	assert.Equal(t, int64(n), atomic.LoadInt64(&grlCalls),
		"global rate limiter should be called once per message, not once per batch")
}
