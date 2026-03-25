package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/devlibx/gox-base/v2"
	errors2 "github.com/devlibx/gox-base/v2/errors"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/devlibx/gox-messaging/v2/noop"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// JobMetadata wraps the actual message payload with retry and visibility information
type JobMetadata struct {
	Payload           string `json:"payload"`
	RemainingAttempts int    `json:"remaining_attempts"`
	TimeoutInMs       int    `json:"timeout_in_ms"`
}

type redisProducer struct {
	config               messaging.ProducerConfig
	redisClient          redis.UniversalClient
	logger               *zap.Logger
	maxAttempts          int
	visibilityTimeout    int
	maxVisibilityTimeout int
	gox.CrossFunction
}

func (p *redisProducer) Send(ctx context.Context, message *messaging.Message) chan *messaging.Response {
	responseChannel := make(chan *messaging.Response, 1)
	defer close(responseChannel)

	payload, err := message.PayloadAsString()
	if err != nil {
		responseChannel <- &messaging.Response{Err: errors2.Wrap(err, "failed to send redis message - cannot read string")}
		return responseChannel
	}

	metadata := JobMetadata{
		Payload:           payload,
		RemainingAttempts: p.maxAttempts,
		TimeoutInMs:       p.visibilityTimeout,
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
	execAt := time.Now().Add(time.Duration(message.MessageDelayInMs) * time.Millisecond).UnixMilli()

	// Calculate TTL: (max_attempts * max_visibility_timeout) + 3 hours safety
	// This ensures the job is cleaned up even if it hangs or the ZADD fails
	ttl := time.Duration(p.maxAttempts)*time.Duration(p.maxVisibilityTimeout)*time.Millisecond + 3*time.Hour

	// Using individual keys per job allows us to set TTLs for automatic cleanup
	jobKey := "job:{" + p.config.Topic + "}:" + jobId
	toProcessKey := "jobs_queue:{" + p.config.Topic + "}:to_process"

	pipe := p.redisClient.Pipeline()
	pipe.Set(ctx, jobKey, metadataBytes, ttl)
	pipe.ZAdd(ctx, toProcessKey, &redis.Z{Score: float64(execAt), Member: jobId})

	_, err = pipe.Exec(ctx)
	if err != nil {
		responseChannel <- &messaging.Response{Err: errors2.Wrap(err, "failed to execute redis pipeline for send")}
	} else {
		responseChannel <- &messaging.Response{Err: nil}
	}

	return responseChannel
}

func (p *redisProducer) Stop() error {
	if p.redisClient != nil {
		return p.redisClient.Close()
	}
	return nil
}

func NewRedisProducer(cf gox.CrossFunction, config messaging.ProducerConfig) (messaging.Producer, error) {
	if !config.Enabled {
		return noop.NewNoOpProducer()
	}
	config.SetupDefaults()
	if config.Endpoint == "" {
		config.Endpoint = "localhost:6379"
	}
	if config.Topic == "" {
		config.Topic = config.Name
	}

	addrs := strings.Split(config.Endpoint, ",")
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: addrs,
	})

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

	p := &redisProducer{
		config:               config,
		redisClient:          client,
		logger:               cf.Logger().With(zap.String("type", "redis"), zap.String("name", config.Name)),
		maxAttempts:          maxAttempts,
		visibilityTimeout:    visibilityTimeout,
		maxVisibilityTimeout: maxVisibilityTimeout,
		CrossFunction:        cf,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis at %s: %w", config.Endpoint, err)
	}

	return p, nil
}


