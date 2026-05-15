package sqs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	goxAws "github.com/devlibx/gox-aws/v2"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	"github.com/devlibx/gox-base/v2/util"
	messaging "github.com/devlibx/gox-messaging/v2"
	"go.uber.org/zap"
)

type sqsBatchConsumer struct {
	sqs    *sqs.SQS
	config messaging.ConsumerConfig
	gox.CrossFunction
	doOnce              sync.Once
	stopDoOnce          sync.Once
	stopConsumerChannel chan bool
	logger              *zap.Logger
}

func (s *sqsBatchConsumer) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	s.doOnce.Do(func() {
		for i := 0; i < s.config.Concurrency; i++ {
			go func() {
				s.internalProcess(ctx, consumeFunction)
			}()
		}
	})
	return nil
}

func (s *sqsBatchConsumer) safeProcess(consumeFunction messaging.ConsumeFunction, message *messaging.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("panic in sqs consumer function: %v", r)
			s.logger.Error("panic in sqs consumer function", zap.String("key", string(message.Key)), zap.Any("payload", message.Payload), zap.String("error", err.Error()))
		}
	}()

	err = consumeFunction.Process(message)
	return
}

func (s *sqsBatchConsumer) safeErrorInProcessing(consumeFunction messaging.ConsumeFunction, message *messaging.Message, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("panic in sqs consumer error function: %v", r)
			s.logger.Error("panic in sqs consumer error function", zap.String("key", string(message.Key)), zap.Any("payload", message.Payload), zap.String("error", err.Error()))
		}
	}()

	consumeFunction.ErrorInProcessing(message, err)
}

func (s *sqsBatchConsumer) internalProcess(ctx context.Context, consumeFunction messaging.ConsumeFunction) {
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

	MaxNumberOfMessages := 1
	if s.config.Properties != nil {
		if val, ok := s.config.Properties["max_number_of_messages"].(int); ok {
			MaxNumberOfMessages = val
		}
	}

	VisibilityTimeout := 0
	if s.config.Properties != nil {
		if val, ok := s.config.Properties["visibility_timeout"].(int); ok {
			VisibilityTimeout = val
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
			input := &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(url),
				WaitTimeSeconds:     aws.Int64(int64(WaitTimeSeconds)),
				MaxNumberOfMessages: aws.Int64(int64(MaxNumberOfMessages)),
			}
			if VisibilityTimeout > 0 {
				input.VisibilityTimeout = aws.Int64(int64(VisibilityTimeout))
			}

			if out, err := s.sqs.ReceiveMessageWithContext(ctx, input); err != nil {
				time.Sleep(1000 * time.Millisecond)
			} else if out.Messages != nil && len(out.Messages) > 0 {
				successfulReceiptHandles := make([]*sqs.DeleteMessageBatchRequestEntry, 0)
				for i, ev := range out.Messages {
					var message *messaging.Message
					if ev.Body != nil {
						message = &messaging.Message{Key: "", Payload: *ev.Body}
					} else {
						message = &messaging.Message{Key: "", Payload: "{}"}
					}

					if err := s.safeProcess(consumeFunction, message); err != nil {
						var ignorable messaging.Ignorable
						if errors.As(err, &ignorable) && ignorable.IsIgnorable() {
							successfulReceiptHandles = append(successfulReceiptHandles, &sqs.DeleteMessageBatchRequestEntry{
								Id:            aws.String(fmt.Sprintf("%d", i)),
								ReceiptHandle: ev.ReceiptHandle,
							})
						} else {
							s.safeErrorInProcessing(consumeFunction, message, err)
						}
					} else {
						successfulReceiptHandles = append(successfulReceiptHandles, &sqs.DeleteMessageBatchRequestEntry{
							Id:            aws.String(fmt.Sprintf("%d", i)),
							ReceiptHandle: ev.ReceiptHandle,
						})
					}
				}

				if len(successfulReceiptHandles) > 0 {
					_, deleteErr := s.sqs.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
						Entries:  successfulReceiptHandles,
						QueueUrl: aws.String(url),
					})
					if deleteErr != nil {
						s.Logger().Error("failed to delete SQS messages in batch", zap.Error(deleteErr))
					} else {
						fmt.Printf("SQS Batch Consumer deleted %d messages\n", len(successfulReceiptHandles))
					}
				}
			}
		}
	}
}

func (s *sqsBatchConsumer) Stop() error {
	s.stopDoOnce.Do(func() {
		s.stopConsumerChannel <- true
		close(s.stopConsumerChannel)
	})
	return nil
}

func (s *sqsBatchConsumer) Logger() *zap.Logger {
	return s.logger
}

func NewSqsBatchConsumer(cf gox.CrossFunction, config messaging.ConsumerConfig) (messaging.Consumer, error) {
	if config.AwsContext == nil {
		config.AwsContext, _ = goxAws.NewAwsContext(cf, config.AwsConfig)
	}

	// Make sure we did get a proper config
	if config.AwsContext == nil || config.AwsContext.GetSession() == nil {
		return nil, errors.New("Sqs config needs AwsContext which is missing here: name=%s", config.Name)
	}

	// Setup defaults if some inputs are missing
	config.SetupDefaults()

	// Build and return
	consumer := sqsBatchConsumer{
		sqs:                 sqs.New(config.AwsContext.GetSession()),
		config:              config,
		CrossFunction:       cf,
		doOnce:              sync.Once{},
		stopDoOnce:          sync.Once{},
		stopConsumerChannel: make(chan bool),
		logger:              cf.Logger().With(zap.String("type", "sqs"), zap.String("mode", "batch")),
	}
	return &consumer, nil
}
