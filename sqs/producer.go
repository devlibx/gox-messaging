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
	"github.com/google/uuid"
	"go.uber.org/zap"
	"sync"
	"time"
)

type sqsProducerV1 struct {
	sqs    *sqs.SQS
	config messaging.ProducerConfig
	gox.CrossFunction
	messageQueue chan internalSendMessage
	closed       chan bool
	closeMutex   *sync.Mutex
	logger       *zap.Logger
}

type internalSendMessage struct {
	message         *messaging.Message
	ctx             context.Context
	responseChannel chan *messaging.Response
}

func (s *sqsProducerV1) internalSend(ctx context.Context, message *messaging.Message) (*sqs.SendMessageOutput, error) {
	data, err := message.PayloadAsString()
	if err != nil {
		return nil, errors.Wrap(err, "failed to creat string from sqs request: request=%v", message)
	}

	// Get SQS url
	url := s.config.Topic
	if util.IsStringEmpty(url) {
		url = s.config.Endpoint
	}

	// This is to generate artificial delay for testing
	if s.config.EnableArtificialDelayToSimulateLatency && message.ArtificialDelayToSimulateLatency > 0 {
		time.Sleep(message.ArtificialDelayToSimulateLatency)
	}

	// For SQS you can add a delay which goes to SQS - event will be visible after this delay
	var messageDelay *int64
	if message.MessageDelayInMs > 0 {
		delay := 1
		if message.MessageDelayInMs < 1000 {
			delay = 1
		} else {
			delay = message.MessageDelayInMs / 1000
		}
		if delay > 15 {
			delay = 15
		}
		messageDelay = aws.Int64(int64(delay))
	}

	// Special handling for SQS FIFO
	// Adding a special handling to make sure we do not break old code
	if message.SqsMessageGroupId != "" {

		// Make sure you do must send some unique ID if it is not given - I expect client to send it
		deduplicationId := message.SqsMessageDeduplicationId
		if util.IsStringEmpty(message.SqsMessageDeduplicationId) {
			deduplicationId = uuid.NewString()
		}

		// Send it over SQS
		if out, err := s.sqs.SendMessageWithContext(ctx, &sqs.SendMessageInput{
			MessageBody:            aws.String(data),
			QueueUrl:               aws.String(url),
			DelaySeconds:           messageDelay,
			MessageGroupId:         aws.String(message.SqsMessageGroupId),
			MessageDeduplicationId: aws.String(deduplicationId),
		}); err != nil {
			return nil, errors.Wrap(err, "failed to send sqs event: message=%v, out=%v", message, out)
		} else {
			return out, nil
		}
	}

	// Send it over SQS
	if out, err := s.sqs.SendMessageWithContext(ctx, &sqs.SendMessageInput{
		MessageBody:  aws.String(data),
		QueueUrl:     aws.String(url),
		DelaySeconds: messageDelay,
	}); err != nil {
		return nil, errors.Wrap(err, "failed to send sqs event: message=%v, out=%v", message, out)
	} else {
		return out, nil
	}
}

func (s *sqsProducerV1) Send(ctx context.Context, message *messaging.Message) chan *messaging.Response {
	// Need a buffered channel - because we are responding back in same goroutine
	responseChannel := make(chan *messaging.Response, 1)

	select {

	case <-ctx.Done():
		responseChannel <- &messaging.Response{Err: errors.Wrap(ctx.Err(), "context is closed to sqs producer: name=%s, message=%s", s.config.Name, message.Key)}
		close(responseChannel)

	case closeMessageChannel, _ := <-s.closed:
		if closeMessageChannel {
			s.Logger().Info("close the producer internal channel", zap.String("name", s.config.Name))
			close(s.messageQueue)
		}
		responseChannel <- &messaging.Response{Err: errors.New("producer is closed: name=%s", s.config.Name)}
		close(responseChannel)

	default:
		s.messageQueue <- internalSendMessage{ctx: ctx, message: message, responseChannel: responseChannel}

	}
	return responseChannel
}

func (s *sqsProducerV1) Stop() error {
	s.closeMutex.Lock()
	defer s.closeMutex.Unlock()
	select {
	case _, closed := <-s.closed:
		if closed {
			return nil
		}
	default:
		s.closed <- true
		close(s.closed)
	}
	return nil
}

// All messages are sent via a send loop - this is to allow sync and async send method to client
func (s *sqsProducerV1) SendLoop() {
L:
	for {
		select {
		case messageToSend, ok := <-s.messageQueue:
			if ok {
				out, err := s.internalSend(messageToSend.ctx, messageToSend.message)
				messageToSend.responseChannel <- &messaging.Response{RawPayload: out, Err: err}
				close(messageToSend.responseChannel)
			} else {
				messageToSend.responseChannel <- &messaging.Response{Err: errors.New("bad")}
				break L
			}
		}
	}
}

func NewSqsProducer(cf gox.CrossFunction, config messaging.ProducerConfig) (messaging.Producer, error) {

	config.AwsContext, _ = goxAws.NewAwsContext(cf, config.AwsConfig)

	// Make sure we did get a proper config
	if config.AwsContext == nil || config.AwsContext.GetSession() == nil {
		return nil, errors.New("Sqs config needs AwsContext which is missing here: name=%s", config.Name)
	}

	// Setup defaults if some inputs are missing
	config.SetupDefaults()

	producer := &sqsProducerV1{
		messageQueue:  make(chan internalSendMessage, config.MaxMessageInBuffer),
		sqs:           sqs.New(config.AwsContext.GetSession()),
		config:        config,
		CrossFunction: cf,
		closed:        make(chan bool, 10),
		closeMutex:    &sync.Mutex{},
		logger:        cf.Logger().With(zap.String("type", "sqs")),
	}

	// Run the send loop
	go func() {
		producer.SendLoop()
		cf.Logger().Info("sqs producer is closed", zap.String("name", config.Name))
	}()

	return producer, nil
}

func (s *sqsProducerV1) Logger() *zap.Logger {
	return s.logger
}
