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
	serviceName := "test-" + uuid.NewString()

	// 1. Setup Producer
	producerConfig := messaging.ProducerConfig{
		Name:                 "test-prod",
		Type:                 "redis",
		Endpoint:             redisEndpoint,
		Topic:                topic,
		Enabled:              true,
		Properties:           map[string]interface{}{"max_attempts": 3},
		Concurrency:          1,
		MandatoryServiceName: serviceName,
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
			"backoff_multiplier":        2.0,
			"batch_size":                10,
		},
		Concurrency:          1,
		MandatoryServiceName: serviceName,
	}
	consumer, err := NewRedisConsumer(cf, consumerConfig)
	assert.NoError(t, err)
	defer consumer.Stop()

	var processedCount int32
	var failedCount int32

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

	// Wait for processing and retries (3 attempts)
	time.Sleep(10 * time.Second)

	// "ok-1" should be processed once
	assert.Equal(t, int32(1), atomic.LoadInt32(&processedCount))

	// "fail-me" should be attempted 3 times (initial + 2 retries) and then dropped
	assert.Equal(t, int32(3), atomic.LoadInt32(&failedCount))

	// Verify Redis is empty for this topic
	p := producer.(*redisProducer)
	keys, _ := p.redisClient.Keys(ctx, serviceName+":jobs:{"+topic+"}:*").Result()
	assert.Equal(t, 0, len(keys))
}

func TestRedisConsumerDelayedMessage(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-delayed-%d", time.Now().UnixNano())
	serviceName := "test-" + uuid.NewString()

	producer, _ := NewRedisProducer(cf, messaging.ProducerConfig{
		Name: "p", Type: "redis", Topic: topic, Enabled: true, Endpoint: redisEndpoint, MandatoryServiceName: serviceName,
	})
	defer producer.Stop()

	consumer, _ := NewRedisConsumer(cf, messaging.ConsumerConfig{
		Name: "c", Type: "redis", Topic: topic, Enabled: true, Endpoint: redisEndpoint, MandatoryServiceName: serviceName,
	})
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

	// Send message with 3s delay
	<-producer.Send(ctx, &messaging.Message{Key: "delayed", Payload: "data", MessageDelayInMs: 3000})

	// Should not be processed immediately
	time.Sleep(1 * time.Second)
	assert.Equal(t, int32(0), atomic.LoadInt32(&processedCount))

	// Wait for mover to move it and consumer to pick it up
	time.Sleep(4 * time.Second)
	assert.Equal(t, int32(1), atomic.LoadInt32(&processedCount))
}

type requeueableError struct {
	delay int64
}

func (e *requeueableError) Error() string { return "requeue me" }
func (e *requeueableError) RequeueAfterMs() (bool, int64) { return true, e.delay }

func TestRedisConsumerRequeueable(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-requeue-%d", time.Now().UnixNano())
	serviceName := "test-" + uuid.NewString()

	producer, _ := NewRedisProducer(cf, messaging.ProducerConfig{
		Name: "p", Type: "redis", Topic: topic, Enabled: true, Endpoint: redisEndpoint, MandatoryServiceName: serviceName,
		Properties: map[string]interface{}{"max_attempts": 2},
	})
	defer producer.Stop()

	consumer, _ := NewRedisConsumer(cf, messaging.ConsumerConfig{
		Name: "c", Type: "redis", Topic: topic, Enabled: true, Endpoint: redisEndpoint, MandatoryServiceName: serviceName,
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
	<-producer.Send(ctx, &messaging.Message{Key: "requeue-test", Payload: "data"})

	// It should be processed 6 times (5 times requeue + 1 time success)
	// even though max_attempts is 2.
	// If it was normal error, it would have been dropped after 2 attempts.
	time.Sleep(2 * time.Second)
	assert.Equal(t, int32(6), atomic.LoadInt32(&processedCount))
}

func BenchmarkRedisConsumerThroughput(b *testing.B) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(b, zap.ErrorLevel)
	topic := fmt.Sprintf("bench-cons-%d", time.Now().UnixNano())
	serviceName := "test-" + uuid.NewString()

	producer, _ := NewRedisProducer(cf, messaging.ProducerConfig{
		Name:                 "p",
		Type:                 "redis",
		Topic:                topic,
		Enabled:              true,
		Endpoint:             redisEndpoint,
		MandatoryServiceName: serviceName,
		Properties: map[string]interface{}{
			"throttle_runnable_job_count":  1000000,
			"throttle_scheduled_job_count": 1000000,
		},
	})

	consumer, _ := NewRedisConsumer(cf, messaging.ConsumerConfig{
		Name:                 "c",
		Type:                 "redis",
		Topic:                topic,
		Enabled:              true,
		Endpoint:             redisEndpoint,
		Concurrency:          20,
		MandatoryServiceName: serviceName,
		Properties:           map[string]interface{}{"batch_size": 100},
	})

	defer func() {
		p := producer.(*redisProducer)
		ctx := context.Background()
		keys, _ := p.redisClient.Keys(ctx, "*"+topic+"*").Result()
		if len(keys) > 0 {
			p.redisClient.Del(ctx, keys...)
		}
		producer.Stop()
		consumer.Stop()
	}()

	count := 10
	inEachLoop := 10000
	consumerWg := &sync.WaitGroup{}
	consumerWg.Add(count * inEachLoop)

	var processed int64
	consumeFunc := &mockConsumeFunction{
		processFunc: func(message *messaging.Message) error {
			atomic.AddInt64(&processed, 1)
			consumerWg.Done()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startToProduce := time.Now()
	wg := sync.WaitGroup{}
	for i := 1; i <= count; i++ {
		wg.Add(1)
		go func(_i int) {
			defer wg.Done()
			for j := 0; j < inEachLoop; j++ {
				<-producer.Send(ctx, &messaging.Message{
					Payload:          "some payload",
					MessageDelayInMs: 2000,
				})
			}
		}(i)
	}
	wg.Wait()

	// Verification 1: Ensure all messages are in the scheduled queue
	p := producer.(*redisProducer)
	scheduledKey := p.getQueueKey("scheduled_jobs")
	runnableKey := p.getQueueKey("runnable_jobs")
	visibilityKey := p.getQueueKey("visibility")

	sCount, _ := p.redisClient.ZCard(ctx, scheduledKey).Result()
	assert.Equal(b, int64(count*inEachLoop), sCount, "Scheduled queue should have all messages before consumer starts")

	fmt.Println("All posted - now lets start consumer... Time taken to produce", time.Since(startToProduce))

	start := time.Now()
	end := time.Now()
	_ = consumer.Process(ctx, consumeFunc)
	consumerWgDone := make(chan struct{}, 10)
	go func() {
		consumerWg.Wait()
		end = time.Now()
		consumerWgDone <- struct{}{}
	}()
	select {
	case <-consumerWgDone:
		fmt.Println("processed:", atomic.LoadInt64(&processed), "time taken = ", end.UnixMilli()-start.UnixMilli())
	case <-time.After(15 * time.Second): // Slightly longer timeout for 50k messages
		fmt.Println("processed: not processed: ", atomic.LoadInt64(&processed))
	}

	// Verification 2: Ensure all queues are empty after processing
	sCount, _ = p.redisClient.ZCard(ctx, scheduledKey).Result()
	rCount, _ := p.redisClient.ZCard(ctx, runnableKey).Result()
	vCount, _ := p.redisClient.ZCard(ctx, visibilityKey).Result()
	assert.Equal(b, int64(0), sCount, "Scheduled queue should be empty")
	assert.Equal(b, int64(0), rCount, "Runnable queue should be empty")
	assert.Equal(b, int64(0), vCount, "Visibility queue should be empty")
}

