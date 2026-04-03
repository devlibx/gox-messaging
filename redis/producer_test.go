package redis

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/devlibx/gox-base/v2/test"
	"github.com/devlibx/gox-base/v2/util"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var redisEndpoint string

func init() {
	flag.StringVar(&redisEndpoint, "real.redis.endpoint", "", "Redis endpoint to use for testing")
}

func TestRedisSend(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-topic-%d", time.Now().UnixNano())
	serviceName := "test-" + uuid.NewString()
	producerConfig := messaging.ProducerConfig{
		Name:                 "test-redis-producer",
		Type:                 "redis",
		Endpoint:             redisEndpoint,
		Topic:                topic,
		Concurrency:          1,
		Enabled:              true,
		Async:                false,
		MandatoryServiceName: serviceName,
	}

	producer, err := NewRedisProducer(cf, producerConfig)
	if err != nil {
		t.Skip("Skipping redis test as redis is not available at", redisEndpoint)
		return
	}
	defer producer.Stop()

	p := producer.(*redisProducer)

	// Test 1 - Test sync message send
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payload := map[string]interface{}{"key": "value"}
	response := <-producer.Send(ctx, &messaging.Message{
		Key:     "msg-1",
		Payload: payload,
	})
	assert.NoError(t, response.Err)

	// Verify in Redis String Key (new naming: jobs:{topic}:{jobId})
	jobKey := "jobs:{" + topic + "}:msg-1"
	val, err := p.redisClient.Get(ctx, jobKey).Result()
	assert.NoError(t, err)
	assert.Contains(t, val, `"payload":"{\"key\":\"value\"}"`)

	// Verify in Runnable Queue (since delay = 0)
	runnableKey := serviceName + ":jobs_queue__runnable_jobs:{" + topic + "}"
	score, err := p.redisClient.ZScore(ctx, runnableKey, "msg-1").Result()
	assert.NoError(t, err)
	assert.NotZero(t, score)

	// Test 2 - Delayed message
	response = <-producer.Send(ctx, &messaging.Message{
		Key:              "msg-delayed",
		Payload:          payload,
		MessageDelayInMs: 2000,
	})
	assert.NoError(t, response.Err)

	// Verify in Scheduled Queue
	scheduledKey := serviceName + ":jobs_queue__scheduled_jobs:{" + topic + "}"
	score, err = p.redisClient.ZScore(ctx, scheduledKey, "msg-delayed").Result()
	assert.NoError(t, err)
	assert.True(t, score > float64(time.Now().UnixMilli()))

	// Test 3 - Concurrent sends
	wg := sync.WaitGroup{}
	count := 10
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			resp := <-producer.Send(ctx, &messaging.Message{
				Key:     fmt.Sprintf("msg-concurrent-%d", id),
				Payload: map[string]interface{}{"id": id},
			})
			assert.NoError(t, resp.Err)
		}(i)
	}
	wg.Wait()

	// Verify count of job data keys
	keys, err := p.redisClient.Keys(ctx, "jobs:{"+topic+"}:*").Result()
	assert.NoError(t, err)
	assert.Equal(t, count+2, len(keys))
}

func TestRedisMandatoryServiceName(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-topic-%d", time.Now().UnixNano())
	serviceName := "my-service-" + uuid.NewString()
	producerConfig := messaging.ProducerConfig{
		Name:                 "test-redis-producer-svc",
		Type:                 "redis",
		Endpoint:             redisEndpoint,
		Topic:                topic,
		Enabled:              true,
		MandatoryServiceName: serviceName,
	}

	producer, err := NewRedisProducer(cf, producerConfig)
	assert.NoError(t, err)
	defer producer.Stop()

	p := producer.(*redisProducer)
	ctx := context.Background()

	payload := map[string]interface{}{"key": "value"}
	response := <-producer.Send(ctx, &messaging.Message{
		Key:     "msg-svc-1",
		Payload: payload,
	})
	assert.NoError(t, response.Err)

	// Verify Runnable Queue with Service Name Prefix
	runnableKey := serviceName + ":jobs_queue__runnable_jobs:{" + topic + "}"
	score, err := p.redisClient.ZScore(ctx, runnableKey, "msg-svc-1").Result()
	assert.NoError(t, err)
	assert.NotZero(t, score)
}

func TestRedisThrottling(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	topic := fmt.Sprintf("test-topic-throttle-%d", time.Now().UnixNano())
	serviceName := "test-" + uuid.NewString()
	producerConfig := messaging.ProducerConfig{
		Name:                 "test-redis-producer-throttle",
		Type:                 "redis",
		Endpoint:             redisEndpoint,
		Topic:                topic,
		Enabled:              true,
		MandatoryServiceName: serviceName,
		Properties: map[string]interface{}{
			"throttle_runnable_job_count":                       2,
			"throttle_delay_ms_after_runnable_job_count_breach": 500,
		},
	}

	producer, err := NewRedisProducer(cf, producerConfig)
	assert.NoError(t, err)
	defer producer.Stop()

	ctx := context.Background()

	// Fill up to threshold
	for i := 0; i < 2; i++ {
		<-producer.Send(ctx, &messaging.Message{
			Key:     fmt.Sprintf("msg-t-%d", i),
			Payload: "data",
		})
	}

	// Next send should be throttled
	start := time.Now()
	<-producer.Send(ctx, &messaging.Message{
		Key:     "msg-throttled",
		Payload: "data",
	})
	elapsed := time.Since(start)

	assert.True(t, elapsed >= 500*time.Millisecond, "Should have been throttled for at least 500ms, took %v", elapsed)
}

func BenchmarkRedisProducerSend(b *testing.B) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(b, zap.ErrorLevel)
	topic := fmt.Sprintf("bench-prod-%d", time.Now().UnixNano())
	serviceName := "test-" + uuid.NewString()
	producerConfig := messaging.ProducerConfig{
		Name:                 "bench-prod",
		Type:                 "redis",
		Endpoint:             redisEndpoint,
		Topic:                topic,
		Enabled:              true,
		MandatoryServiceName: serviceName,
		Properties: map[string]interface{}{
			"throttle_runnable_job_count":  1000000,
			"throttle_scheduled_job_count": 1000000,
		},
	}

	producer, err := NewRedisProducer(cf, producerConfig)
	if err != nil {
		b.Skip("Redis not available")
	}
	defer producer.Stop()

	ctx := context.Background()
	payload := "benchmark-data"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		<-producer.Send(ctx, &messaging.Message{
			Key:     "bench-key",
			Payload: payload,
		})
	}
	b.StopTimer()
}
