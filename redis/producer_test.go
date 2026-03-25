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
	producerConfig := messaging.ProducerConfig{
		Name:        "test-redis-producer",
		Type:        "redis",
		Endpoint:    redisEndpoint,
		Topic:       topic,
		Concurrency: 1,
		Enabled:     true,
		Async:       false,
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

	// Verify in Redis String Key
	jobKey := "job:{" + topic + "}:msg-1"
	val, err := p.redisClient.Get(ctx, jobKey).Result()
	assert.NoError(t, err)
	// Check if the payload is present
	assert.Contains(t, val, `"payload":"{\"key\":\"value\"}"`)
	assert.Contains(t, val, `"remaining_attempts":5`)

	// Test 2 - Delayed message
	response = <-producer.Send(ctx, &messaging.Message{
		Key:              "msg-delayed",
		Payload:          payload,
		MessageDelayInMs: 2000,
	})
	assert.NoError(t, response.Err)

	// Verify in ZSet
	score, err := p.redisClient.ZScore(ctx, "jobs_queue:{"+topic+"}:to_process", "msg-delayed").Result()
	assert.NoError(t, err)
	// Score should be roughly current time + 2000ms
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

	// Verify count
	keys, err := p.redisClient.Keys(ctx, "job:{"+topic+"}:*").Result()
	assert.NoError(t, err)
	assert.Equal(t, count+2, len(keys)) // 1 (msg-1) + 1 (msg-delayed) + 10 (concurrent)
}

func TestRedisStop(t *testing.T) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	producerConfig := messaging.ProducerConfig{
		Name:     "test-redis-producer-stop",
		Type:     "redis",
		Endpoint: redisEndpoint,
		Enabled:  true,
	}

	producer, err := NewRedisProducer(cf, producerConfig)
	if err != nil {
		t.Skip("Skipping redis test")
		return
	}

	err = producer.Stop()
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond) // Let the goroutine finish logging

	// Send after stop should return error
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	resp := <-producer.Send(ctx, &messaging.Message{Payload: "test"})
	assert.Error(t, resp.Err)
	assert.Contains(t, resp.Err.Error(), "client is closed")
}

func BenchmarkRedisThroughput(b *testing.B) {
	if util.IsStringEmpty(redisEndpoint) {
		redisEndpoint = "localhost:6379"
	}

	cf, _ := test.MockCf(b, zap.ErrorLevel)
	topic := fmt.Sprintf("bench-tp-topic-%d", time.Now().UnixNano())
	producerConfig := messaging.ProducerConfig{
		Name:               "bench-tp-redis-producer",
		Type:               "redis",
		Endpoint:           redisEndpoint,
		Topic:              topic,
		Concurrency:        32,
		Enabled:            true,
		MaxMessageInBuffer: 100000,
	}

	producer, err := NewRedisProducer(cf, producerConfig)
	if err != nil {
		b.Skip("Redis not available")
	}
	defer producer.Stop()

	ctx := context.Background()
	payload := map[string]interface{}{"data": "benchmark-throughput"}
	message := &messaging.Message{Payload: payload}

	b.ResetTimer()
	// Set parallelism to ~3 to get ~30 goroutines (assuming GOMAXPROCS is ~10)
	b.SetParallelism(3) 
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			<-producer.Send(ctx, message)
		}
	})
}
