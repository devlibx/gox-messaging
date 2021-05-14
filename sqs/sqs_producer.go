package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	goxAws "github.com/devlibx/gox-aws"
	"github.com/devlibx/gox-base"
	errors2 "github.com/devlibx/gox-base/errors"
	"github.com/devlibx/gox-base/serialization"
	messaging "github.com/devlibx/gox-messaging"
	"go.uber.org/zap"
)

type Config struct {
	QueueUrl         string
	EventConcurrency int
}

type sqsProducer struct {
	sqs    *sqs.SQS
	config Config
	gox.CrossFunction
}

func (s *sqsProducer) Send(request *messaging.Event) (response *messaging.Response, err error) {
	s.Logger().Debug("send SQS message", zap.Any("config", s.config), zap.Any("message", request))

	// Convert to string as a first thing
	data, err := serialization.Stringify(request.Value)
	if err != nil {
		return nil, errors2.Wrap(err, "failed to creat string from sqs request: request=%v", request)
	}

	// Send it over SQS
	out, err := s.sqs.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(data),
		QueueUrl:    aws.String(s.config.QueueUrl),
	})
	if err != nil {
		return nil, errors2.Wrap(err, "failed to send sqs event: request=%v, out=%v", request, out)
	} else {
		// s.Logger().Debug("message send", zap.Any("message", request))
	}

	return &messaging.Response{RawPayload: out}, nil
}

func NewSqsProducer(cf gox.CrossFunction, ctx goxAws.AwsContext, config Config) messaging.Producer {
	queue := sqs.New(ctx.GetSession())
	return &sqsProducer{
		sqs:           queue,
		config:        config,
		CrossFunction: cf,
	}
}
