package pubsub

import (
	"context"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/devlibx/gox-base/v2/errors"
	messaging "github.com/devlibx/gox-messaging/v2"
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
}

func (c *pubSubConsumer) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	c.startDoOnce.Do(func() {
		go func() {
		L:
			for {
				select {
				case <-c.stop:
					break L
				default:
					err := c.subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
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

	// Get project and subscription from config
	project, ok := config.Properties["project"].(string)
	if !ok || project == "" {
		return nil, errors.New("missing pubsub consumer property 'project': name=%s", config.Name)
	}
	subscriptionName, ok := config.Properties["subscription"].(string)
	if !ok || subscriptionName == "" {
		return nil, errors.New("missing pubsub consumer property 'subscription': name=%s", config.Name)
	}

	// Create a new pubsub client
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create pubsub client: name=%s", config.Name)
	}

	// Get a handle to the subscription
	subscription := client.Subscription(subscriptionName)

	// Create a new consumer
	c := &pubSubConsumer{
		config:       config,
		client:       client,
		subscription: subscription,
		logger:       logger.Named("pubsub.consumer").Named(config.Name),
		stop:         make(chan bool),
	}

	return c, nil
}
