package types

import (
	"context"
	"fmt"
	goxAws "github.com/devlibx/gox-aws/v2"
	"github.com/devlibx/gox-base/v2"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/devlibx/gox-messaging/v2/sqs"
	"github.com/google/uuid"
	"os"
	"time"
)

func SqsSendMessage(cf gox.CrossFunction) error {
	awsctx, err := goxAws.NewAwsContext(cf, goxAws.Config{
		// Endpoint: "http://localhost:8000",
		Region: "ap-south-1",
	})
	if err != nil {
		return err
	}

	// Setup 1 - create a producer
	producerConfig := messaging.ProducerConfig{
		Name:        "test_queue",
		Type:        "sqs",
		Topic:       os.Getenv("SQS"),
		Concurrency: 1,
		Enabled:     true,
		AwsContext:  awsctx,
		AwsConfig: goxAws.Config{
			Region: "ap-south-1",
		},
	}

	producer, err := sqs.NewSqsProducer(cf, producerConfig)
	if err != nil {
		return err
	}

	contextWithTimeout, contextCancelFunction := context.WithTimeout(context.Background(), 1*time.Second)
	defer contextCancelFunction()

	// Send a message
	id := uuid.NewString()
	response := <-producer.Send(contextWithTimeout, &messaging.Message{
		Key:              "key-harish-",
		Payload:          map[string]interface{}{"key": "value", "id": id, "time": time.Now().String()},
		MessageDelayInMs: 1000,
	})
	if response.Err != nil {
		return response.Err
	}
	fmt.Println(response.RawPayload)

	consumerConfig := messaging.ConsumerConfig{
		Name:        "test_queue",
		Type:        "sqs",
		Topic:       os.Getenv("SQS"),
		Concurrency: 1,
		Enabled:     true,
		AwsContext:  awsctx,
		AwsConfig: goxAws.Config{
			Region: "ap-south-1",
		},
	}
	consumer, err := sqs.NewSqsConsumer(cf, consumerConfig)
	if err != nil {
		return err
	}
	consumer.Process(context.TODO(), messaging.NewSimpleConsumeFunction(gox.NewNoOpCrossFunction(), "", func(message *messaging.Message) error {
		fmt.Println("Got SQS - ", message, time.Now().String())
		return &_sqsIgnorableError{}
	}, func(message *messaging.Message, err error) {
		fmt.Println("Error in processing message", err)
	}))

	return nil
}

type _sqsIgnorableError struct {
}

func (s *_sqsIgnorableError) Error() string {
	return ""
}

func (s *_sqsIgnorableError) IsIgnorable() bool {
	return true
}
