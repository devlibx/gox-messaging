package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/devlibx/gox-base/v2"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/devlibx/gox-messaging/v2/noop"
	"github.com/go-redis/redis/v8"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
)

const fetchBatchLua = `
local now = tonumber(ARGV[1])
local batch_size = tonumber(ARGV[2])
local visibility_timeout = tonumber(ARGV[3])
local topic = ARGV[4]
local to_process_key = KEYS[1]
local processing_key = KEYS[2]

local job_ids = redis.call('ZRANGEBYSCORE', to_process_key, 0, now, 'LIMIT', 0, batch_size)
local result = {}

if #job_ids > 0 then
    for i, job_id in ipairs(job_ids) do
        redis.call('ZREM', to_process_key, job_id)
        redis.call('ZADD', processing_key, now + visibility_timeout, job_id)
        local job_key = "job:{" .. topic .. "}:" .. job_id
        local payload = redis.call('GET', job_key)
        table.insert(result, job_id)
        table.insert(result, payload or "")
    end
end

return result
`

const consumerAckLua = `
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
return 1
`

const consumerRetryLua = `
local job_id = ARGV[1]
local new_metadata = ARGV[2]
local next_exec_at = tonumber(ARGV[3])
local is_drop = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])

if is_drop == 1 then
    redis.call('DEL', KEYS[3])
    redis.call('ZREM', KEYS[2], job_id)
else
    redis.call('PSETEX', KEYS[3], ttl_ms, new_metadata)
    redis.call('ZREM', KEYS[2], job_id)
    redis.call('ZADD', KEYS[1], next_exec_at, job_id)
end
return 1
`

type redisConsumer struct {
	config      messaging.ConsumerConfig
	redisClient redis.UniversalClient
	ratelimit   ratelimit.Limiter
	logger      *zap.Logger
	startOnce   *sync.Once
	gox.CrossFunction
	stopChan chan bool
}

func (c *redisConsumer) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	c.startOnce.Do(func() {
		// Visibility Watcher
		go c.visibilityWatcher(ctx)

		// Worker Pool
		for i := 0; i < c.config.Concurrency; i++ {
			go c.workerLoop(ctx, consumeFunction)
		}
	})
	return nil
}

func (c *redisConsumer) workerLoop(ctx context.Context, consumeFunction messaging.ConsumeFunction) {
	batchSize := 20
	if val, ok := c.config.Properties["batch_size"].(int); ok {
		batchSize = val
	}
	visibilityTimeout := 30000
	if val, ok := c.config.Properties["visibility_timeout_ms"].(int); ok {
		visibilityTimeout = val
	}

	topic := c.config.Topic
	toProcessKey := "jobs_queue:{" + topic + "}:to_process"
	processingKey := "jobs_queue:{" + topic + "}:processing"

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		default:
			if c.ratelimit != nil {
				c.ratelimit.Take()
			}

			now := time.Now().UnixMilli()
			result, err := c.redisClient.Eval(ctx, fetchBatchLua, []string{toProcessKey, processingKey}, now, batchSize, visibilityTimeout, topic).Result()

			if err != nil {
				c.logger.Error("failed to fetch batch from redis", zap.Error(err))
				time.Sleep(1 * time.Second)
				continue
			}

			items := result.([]interface{})
			if len(items) == 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			for i := 0; i < len(items); i += 2 {
				jobId := items[i].(string)
				metadataStr := items[i+1].(string)

				if metadataStr == "" {
					// Job payload missing (likely expired), clean up ZSet
					c.redisClient.ZRem(ctx, processingKey, jobId)
					continue
				}

				var metadata JobMetadata
				if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
					c.logger.Error("failed to unmarshal job metadata", zap.String("job_id", jobId), zap.Error(err))
					continue
				}

				msg := &messaging.Message{
					Key:     jobId,
					Payload: metadata.Payload,
				}

				err := consumeFunction.Process(msg)
				if err == nil {
					// Atomic ACK - Delete job data and remove from processing
					jobKey := "job:{" + topic + "}:" + jobId
					_, _ = c.redisClient.Eval(ctx, consumerAckLua, []string{processingKey, jobKey}, jobId).Result()
				} else {
					consumeFunction.ErrorInProcessing(msg, err)
					c.logger.Debug("failed to process message, will be retried by watcher", zap.String("job_id", jobId), zap.Error(err))
				}
			}
		}
	}
}

func (c *redisConsumer) visibilityWatcher(ctx context.Context) {
	topic := c.config.Topic
	toProcessKey := "jobs_queue:{" + topic + "}:to_process"
	processingKey := "jobs_queue:{" + topic + "}:processing"

	maxVisibilityTimeout := 300000 // 5 minutes default
	if val, ok := c.config.Properties["max_visibility_timeout_ms"].(int); ok {
		maxVisibilityTimeout = val
	}

	backoffMultiplier := 2.0
	if val, ok := c.config.Properties["backoff_multiplier"].(float64); ok {
		backoffMultiplier = val
	} else if val, ok := c.config.Properties["backoff_multiplier"].(int); ok {
		backoffMultiplier = float64(val)
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		case <-ticker.C:
			now := time.Now().UnixMilli()
			jobs, err := c.redisClient.ZRangeByScore(ctx, processingKey, &redis.ZRangeBy{
				Min: "-inf",
				Max: fmt.Sprintf("%d", now),
			}).Result()

			if err != nil {
				c.logger.Error("failed to get timed out jobs from processing queue", zap.Error(err))
				continue
			}

			for _, jobId := range jobs {
				jobKey := "job:{" + topic + "}:" + jobId
				metadataStr, err := c.redisClient.Get(ctx, jobKey).Result()
				if err != nil {
					// Job missing from storage, clean up ZSet
					c.redisClient.ZRem(ctx, processingKey, jobId)
					continue
				}

				var metadata JobMetadata
				if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
					c.logger.Error("failed to unmarshal job metadata in watcher", zap.String("job_id", jobId), zap.Error(err))
					c.redisClient.ZRem(ctx, processingKey, jobId)
					continue
				}

				metadata.RemainingAttempts--
				if metadata.RemainingAttempts > 0 {
					currentTimeout := float64(metadata.TimeoutInMs)
					nextTimeout := currentTimeout * backoffMultiplier
					if nextTimeout > float64(maxVisibilityTimeout) {
						nextTimeout = float64(maxVisibilityTimeout)
					}
					
					metadata.TimeoutInMs = int(nextTimeout)
					nextExecAt := now + int64(nextTimeout)

					// Calculate remaining TTL for the job key
					remainingTtl := time.Duration(metadata.RemainingAttempts)*time.Duration(maxVisibilityTimeout)*time.Millisecond + 3*time.Hour

					newMetadataBytes, _ := json.Marshal(metadata)
					// Atomic Retry
					_, _ = c.redisClient.Eval(ctx, consumerRetryLua, []string{toProcessKey, processingKey, jobKey}, jobId, newMetadataBytes, nextExecAt, 0, remainingTtl.Milliseconds()).Result()
					c.logger.Info("retrying job", zap.String("job_id", jobId), zap.Int("remaining_attempts", metadata.RemainingAttempts), zap.Int("next_timeout_ms", metadata.TimeoutInMs))
				} else {
					// Atomic Drop
					_, _ = c.redisClient.Eval(ctx, consumerRetryLua, []string{toProcessKey, processingKey, jobKey}, jobId, "", 0, 1, 0).Result()
					c.logger.Info("dropping job after exhausting retries", zap.String("job_id", jobId))
				}
			}
		}
	}
}

func (c *redisConsumer) Stop() error {
	close(c.stopChan)
	if c.redisClient != nil {
		return c.redisClient.Close()
	}
	return nil
}

func NewRedisConsumer(cf gox.CrossFunction, config messaging.ConsumerConfig) (messaging.Consumer, error) {
	if !config.Enabled {
		return noop.NewNoOpConsumer()
	}

	var rl ratelimit.Limiter
	if config.Ratelimit > 0 {
		rl = ratelimit.New(config.Ratelimit)
	} else {
		rl = ratelimit.NewUnlimited()
	}

	if config.Endpoint == "" {
		config.Endpoint = "localhost:6379"
	}
	if config.Topic == "" {
		config.Topic = config.Name
	}
	if config.Concurrency <= 0 {
		config.Concurrency = 1
	}

	addrs := strings.Split(config.Endpoint, ",")
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: addrs,
	})

	c := &redisConsumer{
		config:        config,
		ratelimit:     rl,
		redisClient:   client,
		logger:        cf.Logger().With(zap.String("type", "redis"), zap.String("name", config.Name)),
		startOnce:     &sync.Once{},
		CrossFunction: cf,
		stopChan:      make(chan bool),
	}
	return c, nil
}
