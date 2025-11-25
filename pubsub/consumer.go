package pubsub

import (
	"context"
	noop "github.com/devlibx/gox-messaging/v2/noop"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/devlibx/gox-base/v2/errors"
	messaging "github.com/devlibx/gox-messaging/v2"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
)

type pubSubConsumer struct {
	config       messaging.ConsumerConfig
	client       *pubsub.Client
	subscription *pubsub.Subscription
	logger       *zap.Logger
	stopDoOnce   sync.Once
	stop         chan bool
	startDoOnce  sync.Once
	ratelimit    ratelimit.Limiter
}

func (c *pubSubConsumer) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	c.startDoOnce.Do(func() {
		go func() {
		L:
			for {

				// Ensure Rate limit is applied
				if c.ratelimit != nil {
					c.ratelimit.Take()
				}

				select {
				case <-c.stop:
					break L
				default:
					err := c.subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {

						// Ensure Rate limit is applied
						if c.ratelimit != nil {
							c.ratelimit.Take()
						}

						message := &messaging.Message{
							Key:     msg.ID,
							Payload: msg.Data,
						}
						err := consumeFunction.Process(message)
						if err != nil {
							msg.Nack()
							consumeFunction.ErrorInProcessing(message, err)
						} else {
							msg.Ack()
						}
					})
					if err != nil {
						c.logger.Error("failed to receive messages from pubsub", zap.Error(err))
					}
				}
			}
		}()
	})
	return nil
}

func (c *pubSubConsumer) Stop() error {
	c.stopDoOnce.Do(func() {
		close(c.stop)
	})
	return nil
}

func NewPubSubConsumer(logger *zap.Logger, config messaging.ConsumerConfig) (messaging.Consumer, error) {

	// If disabled then give no-op
	if !config.Enabled {
		return noop.NewNoOpConsumer()
	}

	// Get project and subscription from config
	project, ok := config.Properties["project"].(string)
	if !ok || project == "" {
		return nil, errors.New("missing pubsub consumer property 'project': name=%s", config.Name)
	}

	subscriptionName := config.Topic

	if subscriptionName == "" {
		var ok bool
		subscriptionName, ok = config.Properties["subscription"].(string)
		if !ok || subscriptionName == "" {
			return nil, errors.New("missing pubsub consumer property 'subscription': name=%s", config.Name)
		}
	}

	// Build client
	client, err := buildPubSubClient(ok, config.Properties, project)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create pubsub client: name=%s", config.Name)
	}

	// If rate limit is set then apply it
	var rl ratelimit.Limiter
	if config.Ratelimit > 0 {
		rl = ratelimit.New(config.Ratelimit)
	} else {
		rl = ratelimit.NewUnlimited()
	}

	// Create a new consumer
	c := &pubSubConsumer{
		config:       config,
		client:       client,
		subscription: client.Subscription(subscriptionName),
		logger:       logger.Named("pubsub.consumer").Named(config.Name),
		stop:         make(chan bool),
		ratelimit:    rl,
	}

	return c, nil
}
