package sqs

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	goxAws "github.com/devlibx/gox-aws"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/errors"
	"github.com/devlibx/gox-base/util"
	messaging "github.com/devlibx/gox-messaging"
	"go.uber.org/zap"
	"sync"
	"time"
)

type sqsConsumerV1 struct {
	sqs    *sqs.SQS
	config messaging.ConsumerConfig
	gox.CrossFunction
	doOnce              sync.Once
	stopDoOnce          sync.Once
	stopConsumerChannel chan bool
	logger              *zap.Logger
}

func (s *sqsConsumerV1) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	s.doOnce.Do(func() {
		for i := 0; i < s.config.Concurrency; i++ {
			go func() {
				s.internalProcess(ctx, consumeFunction)
			}()
		}
	})
	return nil
}

func (s *sqsConsumerV1) internalProcess(ctx context.Context, consumeFunction messaging.ConsumeFunction) {
	// Get SQS url
	url := s.config.Topic
	if util.IsStringEmpty(url) {
		url = s.config.Endpoint
	}

	WaitTimeSeconds := 20
	if s.config.Properties != nil {
		if val, ok := s.config.Properties["wait_time_seconds"].(int); ok {
			WaitTimeSeconds = val
		}
	}

L:
	for {
		select {

		case <-s.stopConsumerChannel:
			break L

		case <-ctx.Done():
			break L

		default:
			if out, err := s.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:        aws.String(url),
				WaitTimeSeconds: aws.Int64(int64(WaitTimeSeconds)),
			}); err != nil {
				s.Logger().Debug("timeout")
				time.Sleep(1000 * time.Millisecond)
			} else if out.Messages != nil {
				for _, ev := range out.Messages {

					// Build message to process
					var message *messaging.Message
					if ev.Body != nil {
						message = &messaging.Message{Key: "", Payload: *ev.Body}
					} else {
						message = &messaging.Message{Key: "", Payload: "{}"}
					}

					// Process it and report error if we got some error
					if err := consumeFunction.Process(message); err != nil {
						consumeFunction.ErrorInProcessing(message, err)
					} else {

						// We are done - delete this message from SQS
						_, deleteErr := s.sqs.DeleteMessage(&sqs.DeleteMessageInput{
							QueueUrl:      aws.String(url),
							ReceiptHandle: ev.ReceiptHandle,
						})

						// We reported the error - nothing much can be done here except logging
						if deleteErr != nil {
							if ev.MessageId != nil {
								s.Logger().Error("failed to delete SQS message", zap.String("id", *ev.MessageId))
							} else {
								s.Logger().Error("failed to delete SQS message")
							}
						}

					}
				}
			}
		}
	}
}

func (s *sqsConsumerV1) Stop() error {
	s.stopDoOnce.Do(func() {
		s.stopConsumerChannel <- true
		close(s.stopConsumerChannel)
	})
	return nil
}

func NewSqsConsumer(cf gox.CrossFunction, config messaging.ConsumerConfig) (messaging.Consumer, error) {

	config.AwsContext, _ = goxAws.NewAwsContext(cf, config.AwsConfig)

	// Make sure we did get a proper config
	if config.AwsContext == nil || config.AwsContext.GetSession() == nil {
		return nil, errors.New("Sqs config needs AwsContext which is missing here: name=%s", config.Name)
	}

	// Setup defaults if some inputs are missing
	config.SetupDefaults()

	// Build and return
	consumer := sqsConsumerV1{
		sqs:                 sqs.New(config.AwsContext.GetSession()),
		config:              config,
		CrossFunction:       cf,
		doOnce:              sync.Once{},
		stopDoOnce:          sync.Once{},
		stopConsumerChannel: make(chan bool),
		logger:              cf.Logger().With(zap.String("type", "sqs")),
	}
	return &consumer, nil
}

func (s *sqsConsumerV1) Logger() *zap.Logger {
	return s.logger
}
