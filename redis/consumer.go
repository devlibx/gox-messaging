package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/devlibx/gox-base/v2"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/devlibx/gox-messaging/v2/noop"
	"github.com/redis/go-redis/v9"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
)

/*
fetchBatchLua performs an atomic fetch operation.

WHAT IT DOES:
1. Fetches current Redis server time for accuracy.
2. Identifies up to 'batch_size' ready jobs in the 'runnable' queue.
3. For each found jobId:
  - Removes it from 'runnable'.
  - Adds it to 'visibility' with a new score = current_time + visibility_timeout.
  - Fetches the actual job payload from its global data key.

4. Returns a list of [jobId, payload, jobId, payload, ...] to the Go worker.

KEYS:

	[1] runnable queue key: {service}:jobs_queue__runnable_jobs:{topic}
	[2] visibility queue key: {service}:jobs_queue__visibility:{topic}

ARGV:

	[1] batchSize
	[2] visibilityTimeout
	[3] jobKeyPrefix
*/
const fetchBatchLua = `
local time_res = redis.call('TIME')
local now = (tonumber(time_res[1]) * 1000) + math.floor(tonumber(time_res[2]) / 1000)
local batch_size = tonumber(ARGV[1])
local visibility_timeout = tonumber(ARGV[2])
local job_key_prefix = ARGV[3]
local runnable_key = KEYS[1]
local visibility_key = KEYS[2]

local job_ids = redis.call('ZRANGEBYSCORE', runnable_key, 0, now, 'LIMIT', 0, batch_size)
local result = {}

if #job_ids > 0 then
    for i, job_id in ipairs(job_ids) do
        redis.call('ZREM', runnable_key, job_id)
        redis.call('ZADD', visibility_key, now + visibility_timeout, job_id)
        
        local job_key = job_key_prefix .. job_id
        local payload = redis.call('GET', job_key)
        table.insert(result, job_id)
        table.insert(result, payload or "")
    end
end

return result
`

/*
consumerAckLua performs an atomic acknowledgment.

KEYS:

	[1] visibility queue key: {service}:jobs_queue__visibility:{topic}
	[2] job data key: {service}:jobs:{topic}:{jobId}

ARGV:

	[1] jobId
*/
const consumerAckLua = `
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
return 1
`

/*
consumerRetryLua performs an atomic retry or drop operation.

KEYS:

	[1] runnable queue key: {service}:jobs_queue__runnable_jobs:{topic}
	[2] visibility queue key: {service}:jobs_queue__visibility:{topic}
	[3] job data key: {service}:jobs:{topic}:{jobId}

ARGV:

	[1] jobId
	[2] newMetadataBytes
	[3] delayMs
	[4] isDrop
	[5] ttlMs
*/
const consumerRetryLua = `
local job_id = ARGV[1]
local new_metadata = ARGV[2]
local delay = tonumber(ARGV[3])
local is_drop = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])

local time_res = redis.call('TIME')
local now = (tonumber(time_res[1]) * 1000) + math.floor(tonumber(time_res[2]) / 1000)
local next_exec_at = now + delay

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

/*
requeueLua performs an atomic move from visibility back to runnable with a delay.

KEYS:

	[1] runnable queue key: {service}:jobs_queue__runnable_jobs:{topic}
	[2] visibility queue key: {service}:jobs_queue__visibility:{topic}

ARGV:

	[1] jobId
	[2] delayMs
*/
const requeueLua = `
local job_id = ARGV[1]
local delay = tonumber(ARGV[2])
local time_res = redis.call('TIME')
local now = (tonumber(time_res[1]) * 1000) + math.floor(tonumber(time_res[2]) / 1000)
local next_exec_at = now + delay

redis.call('ZREM', KEYS[2], job_id)
redis.call('ZADD', KEYS[1], next_exec_at, job_id)
return 1
`

/*
moveScheduledLua moves jobs from scheduled to runnable queue.

KEYS:

	[1] scheduled queue key: {service}:jobs_queue__scheduled_jobs:{topic}
	[2] runnable queue key: {service}:jobs_queue__runnable_jobs:{topic}

ARGV:

	[1] current_time
	[2] limit
*/
const moveScheduledLua = `
local time_res = redis.call('TIME')
local now = (tonumber(time_res[1]) * 1000) + math.floor(tonumber(time_res[2]) / 1000)
local scheduled_key = KEYS[1]
local runnable_key = KEYS[2]
local limit = tonumber(ARGV[1])

local job_ids = redis.call('ZRANGEBYSCORE', scheduled_key, 0, now, 'LIMIT', 0, limit)
if #job_ids > 0 then
    for i, job_id in ipairs(job_ids) do
        local score = redis.call('ZSCORE', scheduled_key, job_id)
        redis.call('ZREM', scheduled_key, job_id)
        redis.call('ZADD', runnable_key, score, job_id)
    end
end
return #job_ids
`

/*
recoveryFetchLua identifies jobs that have timed out in the visibility queue.
ARGV[1]: limit
*/
const recoveryFetchLua = `
local time_res = redis.call('TIME')
local now = (tonumber(time_res[1]) * 1000) + math.floor(tonumber(time_res[2]) / 1000)
return redis.call('ZRANGEBYSCORE', KEYS[1], 0, now, 'LIMIT', 0, ARGV[1])
`

type redisConsumer struct {
	config              messaging.ConsumerConfig
	redisClient         redis.UniversalClient
	ratelimit           ratelimit.Limiter
	logger              *zap.Logger
	startOnce           *sync.Once
	isSharedRedisClient bool
	gox.CrossFunction
	stopChan chan bool
}

func (c *redisConsumer) getQueueKey(queueType string) string {
	serviceName := c.config.MandatoryServiceName
	if serviceName == "" {
		serviceName = "default"
	}
	return fmt.Sprintf("%s:jobs_queue__%s:{%s}", serviceName, queueType, c.config.Topic)
}

func (c *redisConsumer) getJobKeyPrefix() string {
	serviceName := c.config.MandatoryServiceName
	if serviceName == "" {
		serviceName = "default"
	}
	return fmt.Sprintf("%s:jobs:{%s}:", serviceName, c.config.Topic)
}

func (c *redisConsumer) getJobKey(jobId string) string {
	return c.getJobKeyPrefix() + jobId
}

func (c *redisConsumer) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	c.startOnce.Do(func() {
		// Scheduled to Runnable Mover
		go c.scheduledJobMover(ctx)

		// Visibility Watcher
		go c.visibilityWatcher(ctx)

		// Worker Pool
		for i := 0; i < c.config.Concurrency; i++ {
			go c.workerLoop(ctx, consumeFunction)
		}
	})
	return nil
}

func (c *redisConsumer) scheduledJobMover(ctx context.Context) {
	scheduledKey := c.getQueueKey("scheduled_jobs")
	runnableKey := c.getQueueKey("runnable_jobs")
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		default:
			// Move up to 1000 jobs at a time (Lua handles its own TIME)
			if val, err := c.redisClient.Eval(ctx, moveScheduledLua, []string{scheduledKey, runnableKey}, 1000).Result(); err != nil {
				c.logger.Error("failed to move scheduled jobs", zap.Error(err))
				time.Sleep(1 * time.Second)
			} else if movedCount, ok := val.(int64); ok && movedCount == 0 {
				// We will sleep for 100 ms if we don't have much work to do
				time.Sleep(100 * time.Millisecond)
			} else if !ok {
				// This should never happen	- safe check to avoid busy loop
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
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

	runnableKey := c.getQueueKey("runnable_jobs")
	visibilityKey := c.getQueueKey("visibility")

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		default:

			jobKeyPrefix := c.getJobKeyPrefix()
			result, err := c.redisClient.Eval(ctx, fetchBatchLua, []string{runnableKey, visibilityKey}, batchSize, visibilityTimeout, jobKeyPrefix).Result()

			if err != nil {
				c.logger.Error("failed to fetch batch from redis", zap.Error(err))
				time.Sleep(1 * time.Second)
				continue
			}

			items := result.([]interface{})
			if len(items) == 0 {
				time.Sleep(100 * time.Millisecond)

				// Just to be safe - we do not spin too much if we have zero records
				if c.ratelimit != nil {
					c.ratelimit.Take()
				}

				continue
			}

			for i := 0; i < len(items); i += 2 {
				jobId := items[i].(string)
				metadataStr := items[i+1].(string)

				if metadataStr == "" {
					// Job payload missing (likely expired), clean up ZSet
					c.redisClient.ZRem(ctx, visibilityKey, jobId)
					continue
				}

				// If global rate limiter is set then call it
				if GlobalRateLimiter != nil {
					if err := GlobalRateLimiter(false, c.config.Name); err != nil {
						c.logger.Warn("global rate limiter failed", zap.String("error", err.Error()))
					}
				}

				if c.ratelimit != nil {
					c.ratelimit.Take()
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
					// Atomic ACK - Delete job data and remove from visibility
					jobKey := c.getJobKey(jobId)
					_, _ = c.redisClient.Eval(ctx, consumerAckLua, []string{visibilityKey, jobKey}, jobId).Result()
				} else {
					// Always call ErrorInProcessing as requested
					consumeFunction.ErrorInProcessing(msg, err)

					var requeueErr messaging.Requeueable
					if errors.As(err, &requeueErr) {
						if shouldRequeue, delay := requeueErr.RequeueAfterMs(); shouldRequeue {
							// Atomic Requeue - moves from visibility to runnable with delay
							// We do NOT update metadata (no retry count decrement)
							_, _ = c.redisClient.Eval(ctx, requeueLua, []string{runnableKey, visibilityKey}, jobId, delay).Result()
							c.logger.Debug("re-queuing job due to Requeueable error", zap.String("job_id", jobId), zap.Int64("delay_ms", delay))
						}
					}

					c.logger.Debug("failed to process message, will be retried by watcher", zap.String("job_id", jobId), zap.Error(err))

					// If the error is throttlable, then we sleep for a bit to slow down the consumer
					var throttleErr messaging.Throttlable
					if errors.As(err, &throttleErr) {
						sleepMs := throttleErr.ThrottleMs()
						if sleepMs > 0 {
							c.logger.Debug("consumer loop will sleep due to Throttlable error", zap.String("job_id", jobId), zap.Int64("sleep_ms", sleepMs))
							time.Sleep(time.Duration(sleepMs) * time.Millisecond)
						}
					}
				}
			}
		}
	}
}

func (c *redisConsumer) visibilityWatcher(ctx context.Context) {
	runnableKey := c.getQueueKey("runnable_jobs")
	visibilityKey := c.getQueueKey("visibility")

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

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		default:
			// Fetch timed out jobs using Lua for server time and efficiency
			res, err := c.redisClient.Eval(ctx, recoveryFetchLua, []string{visibilityKey}, 100).Result()
			if err != nil {
				c.logger.Error("failed to get timed out jobs from visibility queue", zap.Error(err))
				time.Sleep(1 * time.Second)
				continue
			}

			jobs := res.([]interface{})
			if len(jobs) == 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			for _, item := range jobs {
				jobId := item.(string)
				jobKey := c.getJobKey(jobId)
				metadataStr, err := c.redisClient.Get(ctx, jobKey).Result()
				if err != nil {
					// Job missing from storage, clean up ZSet
					c.redisClient.ZRem(ctx, visibilityKey, jobId)
					continue
				}

				var metadata JobMetadata
				if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
					c.logger.Error("failed to unmarshal job metadata in watcher", zap.String("job_id", jobId), zap.Error(err))
					c.redisClient.ZRem(ctx, visibilityKey, jobId)
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

					// Calculate remaining TTL for the job key
					remainingTtl := time.Duration(metadata.RemainingAttempts)*time.Duration(maxVisibilityTimeout)*time.Millisecond + RedisProducerJobKeyTtlBufferInHr

					newMetadataBytes, _ := json.Marshal(metadata)
					// Atomic Retry - moves from visibility to runnable
					_, _ = c.redisClient.Eval(ctx, consumerRetryLua, []string{runnableKey, visibilityKey, jobKey}, jobId, newMetadataBytes, metadata.TimeoutInMs, 0, remainingTtl.Milliseconds()).Result()
					c.logger.Info("retrying job", zap.String("job_id", jobId), zap.Int("remaining_attempts", metadata.RemainingAttempts), zap.Int("next_timeout_ms", metadata.TimeoutInMs))
				} else {
					// Atomic Drop
					_, _ = c.redisClient.Eval(ctx, consumerRetryLua, []string{runnableKey, visibilityKey, jobKey}, jobId, "", 0, 1, 0).Result()
					c.logger.Info("dropping job after exhausting retries", zap.String("job_id", jobId))
				}
			}
		}
	}
}

func (c *redisConsumer) Stop() error {
	close(c.stopChan)
	if c.redisClient != nil && !c.isSharedRedisClient {
		return c.redisClient.Close()
	}
	return nil
}

func NewRedisConsumer(cf gox.CrossFunction, config messaging.ConsumerConfig) (messaging.Consumer, error) {
	if !config.Enabled {
		return noop.NewNoOpConsumer()
	}

	if config.MandatoryServiceName == "" {
		return nil, fmt.Errorf("mandatory_service_name property must be set for type redis")
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

	var client redis.UniversalClient
	isShared := false
	if config.RedisClient != nil {
		if c, ok := config.RedisClient.(redis.UniversalClient); ok {
			client = c
			isShared = true
		}
	}

	if client == nil {
		var err error
		client, err = CreateRedisUniversalClient(config.Endpoint, config.Properties)
		if err != nil {
			return nil, err
		}
	}

	c := &redisConsumer{
		config:              config,
		ratelimit:           rl,
		redisClient:         client,
		logger:              cf.Logger().With(zap.String("type", "redis"), zap.String("name", config.Name)),
		startOnce:           &sync.Once{},
		isSharedRedisClient: isShared,
		CrossFunction:       cf,
		stopChan:            make(chan bool),
	}
	return c, nil
}
