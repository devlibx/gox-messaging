package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/devlibx/gox-base/v2/errors"
	messaging "github.com/devlibx/gox-messaging/v2"
	"go.uber.org/zap"
	"sync"
)

type pubSubProducer struct {
	sync.Mutex
	config     messaging.ProducerConfig
	producer   *pubsub.Topic
	client     *pubsub.Client
	logger     *zap.Logger
	stopDoOnce sync.Once
	stop       chan bool
}

func (p *pubSubProducer) Send(ctx context.Context, message *messaging.Message) chan *messaging.Response {
	responseChannel := make(chan *messaging.Response, 1)

	// Get payload as bytes
	payload, err := message.PayloadAsBytes()
	if err != nil {
		responseChannel <- &messaging.Response{Err: errors.Wrap(err, "failed to send pubsub message - cannot read bytes")}
		return responseChannel
	}

	// Publish the message
	result := p.producer.Publish(ctx, &pubsub.Message{
		Data: payload,
	})

	// Wait for the result
	go func() {
		id, err := result.Get(ctx)
		if err != nil {
			responseChannel <- &messaging.Response{Err: errors.Wrap(err, "failed to send pubsub message")}
		} else {
			responseChannel <- &messaging.Response{RawPayload: id}
		}
	}()

	return responseChannel
}

func (p *pubSubProducer) Stop() error {
	p.stopDoOnce.Do(func() {
		p.producer.Stop()
	})
	return nil
}

func NewPubSubProducer(logger *zap.Logger, config messaging.ProducerConfig) (messaging.Producer, error) {

	// Get project and topic from config
	project, ok := config.Properties["project"].(string)
	if !ok || project == "" {
		return nil, errors.New("missing pubsub producer property 'project': name=%s", config.Name)
	}

	// Create a new pubsub client
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create pubsub client: name=%s", config.Name)
	}

	// Get a handle to the topic
	topic := client.Topic(config.Topic)

	// Create a new producer
	p := &pubSubProducer{
		config:   config,
		producer: topic,
		client:   client,
		logger:   logger.Named("pubsub.producer").Named(config.Name),
		stop:     make(chan bool),
	}

	return p, nil
}
