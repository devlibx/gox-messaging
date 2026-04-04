package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/devlibx/gox-base/v2"
	errors2 "github.com/devlibx/gox-base/v2/errors"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/devlibx/gox-messaging/v2/noop"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// JobMetadata wraps the actual message payload with retry and visibility information
type JobMetadata struct {
	Payload           string `json:"payload"`
	RemainingAttempts int    `json:"remaining_attempts"`
	TimeoutInMs       int    `json:"timeout_in_ms"`
	Priority          int    `json:"priority"`
}

/*
sendLuaScript performs an atomic send operation.

WHY LUA?
 1. ATOMICITY: We must ensure that a job's metadata (SET) and its queue entry (ZADD) are
    created together.
 2. CONSISTENCY: Uses Redis server time for scheduling.
 3. PERFORMANCE: Returns both scheduled and runnable counts for throttling.

KEYS:

	[1] job data key: {service}:jobs:{topic}:{jobId}
	[2] scheduled queue key: {service}:jobs_queue__scheduled_jobs:{topic}
	[3] runnable queue key: {service}:jobs_queue__runnable_jobs:{topic}

ARGV:

	[1] metadataBytes
	[2] ttlMilliseconds
	[3] messageDelayMs
	[4] jobId
*/
const sendLuaScript = `
local time_res = redis.call('TIME')
local current_time = (tonumber(time_res[1]) * 1000) + math.floor(tonumber(time_res[2]) / 1000)
local delay = tonumber(ARGV[3])
local exec_at = current_time + delay
local jobId = ARGV[4]
local priority = tonumber(ARGV[5])
local priority_enabled = tonumber(ARGV[6])

redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])

if delay > 0 then
    redis.call('ZADD', KEYS[2], exec_at, jobId)
else
    local score = current_time
    if priority_enabled == 1 then
        score = priority * 1000000000
    end
    redis.call('ZADD', KEYS[3], score, jobId)
end

return {redis.call('ZCARD', KEYS[2]), redis.call('ZCARD', KEYS[3])}
`

type redisProducer struct {
	config               messaging.ProducerConfig
	redisClient          redis.UniversalClient
	logger               *zap.Logger
	maxAttempts          int
	visibilityTimeout    int
	maxVisibilityTimeout int
	priorityEnabled      bool
	isSharedRedisClient  bool
	gox.CrossFunction

	// Throttling properties
	throttleScheduledJobCount                   int
	throttleRunnableJobCount                    int
	throttleDelayMsAfterScheduledJobCountBreach int
	throttleDelayMsAfterRunnableJobCountBreach  int
	lastScheduledCount                          int64
	lastRunnableCount                           int64
	countMutex                                  sync.RWMutex
}

func (p *redisProducer) getQueueKey(queueType string) string {
	serviceName := p.config.MandatoryServiceName
	if serviceName == "" {
		serviceName = "default"
	}
	if p.priorityEnabled {
		return fmt.Sprintf("%s:jobs_queue__%s_prio:{%s}", serviceName, queueType, p.config.Topic)
	}
	return fmt.Sprintf("%s:jobs_queue__%s:{%s}", serviceName, queueType, p.config.Topic)
}

func (p *redisProducer) getJobKeyPrefix() string {
	serviceName := p.config.MandatoryServiceName
	if serviceName == "" {
		serviceName = "default"
	}
	return fmt.Sprintf("%s:jobs:{%s}:", serviceName, p.config.Topic)
}

func (p *redisProducer) getJobKey(jobId string) string {
	return p.getJobKeyPrefix() + jobId
}

func (p *redisProducer) Send(ctx context.Context, message *messaging.Message) chan *messaging.Response {
	responseChannel := make(chan *messaging.Response, 1)
	defer close(responseChannel)

	defer func() {
		tags := map[string]string{"type": "redis", "topic": p.config.Topic, "service": p.config.MandatoryServiceName}
		p.Metric().Tagged(tags).Gauge("redis_producer_scheduled_count").Update(float64(p.lastScheduledCount))
		p.Metric().Tagged(tags).Gauge("redis_producer_runnable_count").Update(float64(p.lastRunnableCount))
	}()

	// Throttling check using last known counts
	p.countMutex.RLock()
	scheduledCount := p.lastScheduledCount
	runnableCount := p.lastRunnableCount
	p.countMutex.RUnlock()

	if p.throttleScheduledJobCount > 0 && scheduledCount >= int64(p.throttleScheduledJobCount) {
		time.Sleep(time.Duration(p.throttleDelayMsAfterScheduledJobCountBreach) * time.Millisecond)
	}
	if p.throttleRunnableJobCount > 0 && runnableCount >= int64(p.throttleRunnableJobCount) {
		time.Sleep(time.Duration(p.throttleDelayMsAfterRunnableJobCountBreach) * time.Millisecond)
	}

	payload, err := message.PayloadAsString()
	if err != nil {
		responseChannel <- &messaging.Response{Err: errors2.Wrap(err, "failed to send redis message - cannot read string")}
		return responseChannel
	}

	metadata := JobMetadata{
		Payload:           payload,
		RemainingAttempts: p.maxAttempts,
		TimeoutInMs:       p.visibilityTimeout,
		Priority:          message.Priority,
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		responseChannel <- &messaging.Response{Err: errors2.Wrap(err, "failed to marshal job metadata")}
		return responseChannel
	}

	jobId := uuid.NewString()
	if message.Key != "" {
		jobId = message.Key
	}

	// Calculate TTL: (max_attempts * max_visibility_timeout) + 3 hours safety
	ttl := time.Duration(p.maxAttempts)*time.Duration(p.maxVisibilityTimeout)*time.Millisecond + 3*time.Hour

	jobKey := p.getJobKey(jobId)
	scheduledKey := p.getQueueKey("scheduled_jobs")
	runnableKey := p.getQueueKey("runnable_jobs")

	priorityEnabledFlag := 0
	if p.priorityEnabled {
		priorityEnabledFlag = 1
	}

	res, err := p.redisClient.Eval(ctx, sendLuaScript, []string{jobKey, scheduledKey, runnableKey},
		metadataBytes, ttl.Milliseconds(), message.MessageDelayInMs, jobId, message.Priority, priorityEnabledFlag).Result()

	if err != nil {
		responseChannel <- &messaging.Response{Err: errors2.Wrap(err, "failed to execute redis lua script for send")}
	} else {
		// Update counts from Lua return value
		if counts, ok := res.([]interface{}); ok && len(counts) == 2 {
			p.countMutex.Lock()
			p.lastScheduledCount = counts[0].(int64)
			p.lastRunnableCount = counts[1].(int64)
			p.countMutex.Unlock()
		}
		responseChannel <- &messaging.Response{Err: nil}
	}

	return responseChannel
}

func (p *redisProducer) Stop() error {
	if p.redisClient != nil && !p.isSharedRedisClient {
		return p.redisClient.Close()
	}
	return nil
}

func NewRedisProducer(cf gox.CrossFunction, config messaging.ProducerConfig) (messaging.Producer, error) {
	if !config.Enabled {
		return noop.NewNoOpProducer()
	}
	config.SetupDefaults()

	if config.MandatoryServiceName == "" {
		return nil, fmt.Errorf("mandatory_service_name property must be set for type redis")
	}

	if config.Endpoint == "" {
		config.Endpoint = "localhost:6379"
	}
	if config.Topic == "" {
		config.Topic = config.Name
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

	maxAttempts := 5
	if val, ok := config.Properties["max_attempts"].(int); ok {
		maxAttempts = val
	}

	visibilityTimeout := 30000
	if val, ok := config.Properties["visibility_timeout_ms"].(int); ok {
		visibilityTimeout = val
	}

	maxVisibilityTimeout := 300000 // 5 minutes default
	if val, ok := config.Properties["max_visibility_timeout_ms"].(int); ok {
		maxVisibilityTimeout = val
	}

	// Throttling properties
	throttleScheduledJobCount := 10000
	if val, ok := config.Properties["throttle_scheduled_job_count"].(int); ok {
		throttleScheduledJobCount = val
	} else if val, ok := config.Properties["throttle_scheduled_job_count"].(float64); ok {
		throttleScheduledJobCount = int(val)
	}

	throttleRunnableJobCount := 10000
	if val, ok := config.Properties["throttle_runnable_job_count"].(int); ok {
		throttleRunnableJobCount = val
	} else if val, ok := config.Properties["throttle_runnable_job_count"].(float64); ok {
		throttleRunnableJobCount = int(val)
	}

	throttleDelayMsScheduled := 5
	if val, ok := config.Properties["throttle_delay_ms_after_scheduled_job_count_breach"].(int); ok {
		throttleDelayMsScheduled = val
	} else if val, ok := config.Properties["throttle_delay_ms_after_scheduled_job_count_breach"].(float64); ok {
		throttleDelayMsScheduled = int(val)
	}

	throttleDelayMsRunnable := 5
	if val, ok := config.Properties["throttle_delay_ms_after_runnable_job_count_breach"].(int); ok {
		throttleDelayMsRunnable = val
	} else if val, ok := config.Properties["throttle_delay_ms_after_runnable_job_count_breach"].(float64); ok {
		throttleDelayMsRunnable = int(val)
	}

	priorityEnabled, _ := config.Properties["priority_enabled"].(bool)

	p := &redisProducer{
		config:                    config,
		redisClient:               client,
		logger:                    cf.Logger().With(zap.String("type", "redis"), zap.String("name", config.Name)),
		maxAttempts:               maxAttempts,
		visibilityTimeout:         visibilityTimeout,
		maxVisibilityTimeout:      maxVisibilityTimeout,
		priorityEnabled:           priorityEnabled,
		isSharedRedisClient:       isShared,
		CrossFunction:             cf,
		throttleScheduledJobCount: throttleScheduledJobCount,
		throttleRunnableJobCount:  throttleRunnableJobCount,
		throttleDelayMsAfterScheduledJobCountBreach: throttleDelayMsScheduled,
		throttleDelayMsAfterRunnableJobCountBreach:  throttleDelayMsRunnable,
	}

	// Fetch initial counts
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	scheduledKey := p.getQueueKey("scheduled_jobs")
	runnableKey := p.getQueueKey("runnable_jobs")
	sCount, _ := p.redisClient.ZCard(ctx, scheduledKey).Result()
	rCount, _ := p.redisClient.ZCard(ctx, runnableKey).Result()
	p.lastScheduledCount = sCount
	p.lastRunnableCount = rCount

	if err := p.redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis at %s: %w", config.Endpoint, err)
	}

	return p, nil
}
