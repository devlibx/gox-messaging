package sqs

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	goxAws "github.com/devlibx/gox-aws"
	"github.com/devlibx/gox-aws/messaging"
	"github.com/devlibx/gox-base"
	"go.uber.org/zap"
	"time"
)

type sqsConsumer struct {
	sqs    *sqs.SQS
	config Config
	gox.CrossFunction
}

func (s *sqsConsumer) readEvents(ctx context.Context) chan messaging.Event {
	readChannel := make(chan messaging.Event, s.config.EventConcurrency)
	go func() {
		for {

			// Break loop if context is closed
			select {
			case <-ctx.Done():
				close(readChannel)
				return
			default: // No Op - this is selected if context is not done i.e. not closed
			}

			// Read messages from queue
			out, err := s.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:        aws.String(s.config.QueueUrl),
				WaitTimeSeconds: aws.Int64(1),
			})

			// Got some error - wait and try after 10ms
			if err != nil || out.Messages == nil {
				time.Sleep(10 * time.Millisecond)
				continue
			}

			// Give back the messages in read channel
			for _, ev := range out.Messages {
				if ev.Body != nil {
					readChannel <- messaging.Event{Key: "", Value: *ev.Body, RawEvent: ev}
				} else {
					readChannel <- messaging.Event{Key: "", Value: "{}", RawEvent: ev}
				}
			}
		}
	}()
	return readChannel
}

func (s *sqsConsumer) Process(ctx context.Context, messagePressedAckChannel chan messaging.Event) (chan messaging.Event, error) {
	eventChannel := make(chan messaging.Event, s.config.EventConcurrency)

	go func() {
		// Read messages from SQS
		readChannel := s.readEvents(ctx)

		//Read messages
		for {
			select {
			case <-ctx.Done():
				close(eventChannel)
				return

			case ev, ok := <-readChannel:
				if ok {
					eventChannel <- ev
				} else {
					close(eventChannel)
					return
				}
			}
		}
	}()

	// Delete SQS message
	go func() {
		for ev := range messagePressedAckChannel {

			if ev.RawEvent == nil {
				s.Logger().Error("SQS message is sent for deletion but the RawEvent is missing")
				continue
			}

			if awsSqsEvent, ok := ev.RawEvent.(*sqs.Message); ok {
				_, err := s.sqs.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      aws.String(s.config.QueueUrl),
					ReceiptHandle: awsSqsEvent.ReceiptHandle,
				})
				if err != nil {
					if awsSqsEvent.MessageId != nil {
						s.Logger().Error("failed to delete SQS message", zap.String("id", *awsSqsEvent.MessageId))
					} else {
						s.Logger().Error("failed to delete SQS message")
					}
				}
			}
		}
	}()

	return eventChannel, nil
}

func NewSqsConsumer(cf gox.CrossFunction, ctx goxAws.AwsContext, config Config) messaging.Consumer {
	if config.EventConcurrency < 0 {
		config.EventConcurrency = 100
	}
	return &sqsConsumer{
		sqs:           sqs.New(ctx.GetSession()),
		config:        config,
		CrossFunction: cf,
	}
}
