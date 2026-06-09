package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/devlibx/gox-base/v2"
	messaging "github.com/devlibx/gox-messaging/v2"
	"go.uber.org/zap"
)

/*
moveScheduledPrioLua moves jobs from scheduled to runnable queue with priority-based scoring.

KEYS:

	[1] scheduled queue key
	[2] runnable queue key

ARGV:

	[1] limit
	[2] job_key_prefix
*/
const moveScheduledPrioLua = `
local time_res = redis.call('TIME')
local now = (tonumber(time_res[1]) * 1000) + math.floor(tonumber(time_res[2]) / 1000)
local scheduled_key = KEYS[1]
local runnable_key = KEYS[2]
local limit = tonumber(ARGV[1])
local job_key_prefix = ARGV[2]

local job_ids = redis.call('ZRANGEBYSCORE', scheduled_key, 0, now, 'LIMIT', 0, limit)
if #job_ids > 0 then
    for i, job_id in ipairs(job_ids) do
        local job_key = job_key_prefix .. job_id
        local metadata_str = redis.call('GET', job_key)
        local priority = 0
        if metadata_str then
            local p = string.match(metadata_str, '"priority":(%d+)')
            if p then priority = tonumber(p) end
        end
        -- Score is priority * 1B. 
        local score = priority * 1000000000
        
        redis.call('ZREM', scheduled_key, job_id)
        redis.call('ZADD', runnable_key, score, job_id)
    end
end
return #job_ids
`

/*
consumerRetryPrioLua performs an atomic retry or drop operation with priority support.

KEYS:

	[1] scheduled queue key
	[2] runnable queue key
	[3] visibility queue key
	[4] job data key

ARGV:

	[1] jobId
	[2] newMetadataBytes
	[3] delayMs
	[4] isDrop
	[5] ttlMs
	[6] priority
*/
const consumerRetryPrioLua = `
local job_id = ARGV[1]
local new_metadata = ARGV[2]
local delay = tonumber(ARGV[3])
local is_drop = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])
local priority = tonumber(ARGV[6])

local time_res = redis.call('TIME')
local now = (tonumber(time_res[1]) * 1000) + math.floor(tonumber(time_res[2]) / 1000)

if is_drop == 1 then
    redis.call('DEL', KEYS[4])
    redis.call('ZREM', KEYS[3], job_id)
else
    redis.call('PSETEX', KEYS[4], ttl_ms, new_metadata)
    redis.call('ZREM', KEYS[3], job_id)
    
    if delay > 0 then
        -- Put back to scheduled queue with timestamp
        local next_exec_at = now + delay
        redis.call('ZADD', KEYS[1], next_exec_at, job_id)
    else
        -- Put back to runnable queue with priority score
        local score = priority * 1000000000
        redis.call('ZADD', KEYS[2], score, job_id)
    end
end
return 1
`

/*
fetchBatchPrioLua performs an atomic fetch operation using priority (ZRANGE).

KEYS:

	[1] runnable queue key
	[2] visibility queue key

ARGV:

	[1] batchSize
	[2] visibilityTimeout
	[3] jobKeyPrefix
*/
const fetchBatchPrioLua = `
local time_res = redis.call('TIME')
local now = (tonumber(time_res[1]) * 1000) + math.floor(tonumber(time_res[2]) / 1000)
local batch_size = tonumber(ARGV[1])
local visibility_timeout = tonumber(ARGV[2])
local job_key_prefix = ARGV[3]
local runnable_key = KEYS[1]
local visibility_key = KEYS[2]

-- Use ZRANGE to pick jobs by priority (lowest score first)
local job_ids = redis.call('ZRANGE', runnable_key, 0, batch_size - 1)
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
requeuePrioLua performs an atomic move from visibility back to scheduled queue with a delay.

KEYS:

	[1] scheduled queue key
	[2] visibility queue key

ARGV:

	[1] jobId
	[2] delayMs
*/
const requeuePrioLua = `
local job_id = ARGV[1]
local delay = tonumber(ARGV[2])
local time_res = redis.call('TIME')
local now = (tonumber(time_res[1]) * 1000) + math.floor(tonumber(time_res[2]) / 1000)
local next_exec_at = now + delay

redis.call('ZREM', KEYS[2], job_id)
redis.call('ZADD', KEYS[1], next_exec_at, job_id)
return 1
`

type redisPriorityConsumer struct {
	*redisConsumer
}

func (c *redisPriorityConsumer) getQueueKey(queueType string) string {
	serviceName := c.config.MandatoryServiceName
	if serviceName == "" {
		serviceName = "default"
	}
	// Add _prio suffix to queue types
	return fmt.Sprintf("%s:jobs_queue__%s_prio:{%s}", serviceName, queueType, c.config.Topic)
}

func (c *redisPriorityConsumer) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	c.startOnce.Do(func() {
		// Scheduled to Runnable Mover (Priority Aware)
		go c.scheduledJobMover(ctx)

		// Visibility Watcher (Priority Aware)
		go c.visibilityWatcher(ctx)

		// Worker Pool
		for i := 0; i < c.config.Concurrency; i++ {
			go c.workerLoop(ctx, consumeFunction)
		}
	})
	return nil
}

func (c *redisPriorityConsumer) workerLoop(ctx context.Context, consumeFunction messaging.ConsumeFunction) {
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
	scheduledKey := c.getQueueKey("scheduled_jobs")

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		default:

			jobKeyPrefix := c.getJobKeyPrefix()
			// Use fetchBatchPrioLua instead of fetchBatchLua
			result, err := c.redisClient.Eval(ctx, fetchBatchPrioLua, []string{runnableKey, visibilityKey}, batchSize, visibilityTimeout, jobKeyPrefix).Result()

			if err != nil {
				c.logger.Error("failed to fetch batch from redis (prio)", zap.Error(err))
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
				if c.ratelimit != nil {
					c.ratelimit.Take()
				}

				jobId := items[i].(string)
				metadataStr := items[i+1].(string)

				if metadataStr == "" {
					c.redisClient.ZRem(ctx, visibilityKey, jobId)
					c.logger.Warn("TTL reached for redis (ignore job)", zap.String("jobKeyPrefix", jobKeyPrefix), zap.String("jobKeyPrefix", jobKeyPrefix), zap.String("job_id", jobId))
					continue
				}

				var metadata JobMetadata
				if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
					c.logger.Error("failed to unmarshal job metadata", zap.String("job_id", jobId), zap.Error(err))
					continue
				}

				msg := &messaging.Message{
					Key:      jobId,
					Payload:  metadata.Payload,
					Priority: metadata.Priority,
				}

				err := consumeFunction.Process(msg)
				if err == nil {
					jobKey := c.getJobKey(jobId)
					_, _ = c.redisClient.Eval(ctx, consumerAckLua, []string{visibilityKey, jobKey}, jobId).Result()
				} else {
					// Always call ErrorInProcessing as requested
					consumeFunction.ErrorInProcessing(msg, err)

					var requeueErr messaging.Requeueable
					if errors.As(err, &requeueErr) {
						if shouldRequeue, delay := requeueErr.RequeueAfterMs(); shouldRequeue {
							// Atomic Requeue - moves from visibility to scheduled with delay
							// We do NOT update metadata (no retry count decrement)
							_, _ = c.redisClient.Eval(ctx, requeuePrioLua, []string{scheduledKey, visibilityKey}, jobId, delay).Result()
							c.logger.Debug("re-queuing job due to Requeueable error (prio)", zap.String("job_id", jobId), zap.Int64("delay_ms", delay))
						}
					}

					c.logger.Debug("failed to process message, will be retried by watcher (prio)", zap.String("job_id", jobId), zap.Error(err))

					// If the error is throttlable, then we sleep for a bit to slow down the consumer
					var throttleErr messaging.Throttlable
					if errors.As(err, &throttleErr) {
						sleepMs := throttleErr.ThrottleMs()
						if sleepMs > 0 {
							c.logger.Debug("consumer loop will sleep due to Throttlable error (prio)", zap.String("job_id", jobId), zap.Int64("sleep_ms", sleepMs))
							time.Sleep(time.Duration(sleepMs) * time.Millisecond)
						}
					}
				}
			}
		}
	}
}

func (c *redisPriorityConsumer) scheduledJobMover(ctx context.Context) {
	scheduledKey := c.getQueueKey("scheduled_jobs")
	runnableKey := c.getQueueKey("runnable_jobs")
	jobKeyPrefix := c.getJobKeyPrefix()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		default:
			if val, err := c.redisClient.Eval(ctx, moveScheduledPrioLua, []string{scheduledKey, runnableKey}, 1000, jobKeyPrefix).Result(); err != nil {
				c.logger.Error("failed to move scheduled jobs (prio)", zap.Error(err))
				time.Sleep(1 * time.Second)
			} else if movedCount, ok := val.(int64); ok && movedCount == 0 {
				time.Sleep(100 * time.Millisecond)
			} else if !ok {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func (c *redisPriorityConsumer) visibilityWatcher(ctx context.Context) {
	scheduledKey := c.getQueueKey("scheduled_jobs")
	runnableKey := c.getQueueKey("runnable_jobs")
	visibilityKey := c.getQueueKey("visibility")

	maxVisibilityTimeout := 300000
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
					remainingTtl := time.Duration(metadata.RemainingAttempts)*time.Duration(maxVisibilityTimeout)*time.Millisecond + RedisProducerJobKeyTtlBufferInHr

					newMetadataBytes, _ := json.Marshal(metadata)
					// Atomic Retry - uses scheduledKey and runnableKey
					_, _ = c.redisClient.Eval(ctx, consumerRetryPrioLua,
						[]string{scheduledKey, runnableKey, visibilityKey, jobKey},
						jobId, newMetadataBytes, metadata.TimeoutInMs, 0, remainingTtl.Milliseconds(), metadata.Priority).Result()

					c.logger.Info("retrying job (prio)", zap.String("job_id", jobId), zap.Int("priority", metadata.Priority))
				} else {
					// Atomic Drop
					_, _ = c.redisClient.Eval(ctx, consumerRetryPrioLua,
						[]string{scheduledKey, runnableKey, visibilityKey, jobKey},
						jobId, "", 0, 1, 0, 0).Result()
					c.logger.Info("dropping job after exhausting retries (prio)", zap.String("job_id", jobId))
				}
			}
		}
	}
}

func NewRedisPriorityConsumer(cf gox.CrossFunction, config messaging.ConsumerConfig) (messaging.Consumer, error) {
	base, err := NewRedisConsumer(cf, config)
	if err != nil {
		return nil, err
	}

	if rb, ok := base.(*redisConsumer); ok {
		return &redisPriorityConsumer{
			redisConsumer: rb,
		}, nil
	}
	return base, nil
}
