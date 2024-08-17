package sqs

import (
	"context"
	goxAws "github.com/devlibx/gox-aws/v2"
	"github.com/devlibx/gox-base/v2/test"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
	"time"
)

func Test_RunWithSQS(t *testing.T) {
	// Make it false to run this test
	// you need to run SQS in local. Use following command
	// docker run --rm -it -p 8000:4566 -p 4571:4571 localstack/localstack
	if true {
		t.Skip("test needs SQS to be running. Ignore this test...")
	}
	cf, _ := test.MockCf(t)

	awsctx, err := goxAws.NewAwsContext(cf, goxAws.Config{
		Endpoint: "http://localhost:8000",
		Region:   "us-east-1",
	})
	assert.NoError(t, err)

	producerConfig := messaging.ProducerConfig{
		Name:                                   "test_queue",
		Type:                                   "sqs",
		Topic:                                  "http://localhost:8000/000000000000/test_queue_in_sqs",
		Concurrency:                            1,
		Enabled:                                true,
		Properties:                             nil,
		Async:                                  false,
		AwsContext:                             awsctx,
		EnableArtificialDelayToSimulateLatency: true,
	}

	producer, err := NewSqsProducer(cf, producerConfig)
	assert.NoError(t, err)

	// Test 1 - Test sync message send
	id := uuid.NewString()
	c, _ := context.WithTimeout(context.Background(), 1*time.Second)
	response := <-producer.Send(c, &messaging.Message{
		Key:     "key",
		Payload: map[string]interface{}{"key": "value", "id": id},
	})
	assert.NoError(t, response.Err)
	assert.NotNil(t, response.RawPayload)
	cf.Logger().Debug("Output from SQS", zap.Any("sqsResponse", response.RawPayload))

	consumerConfig := messaging.ConsumerConfig{
		Name:        producerConfig.Name,
		Type:        "sqs",
		Topic:       producerConfig.Topic,
		Concurrency: 2,
		Enabled:     true,
		Properties:  nil,
		AwsContext:  awsctx,
	}
	// Test 1 - Read message
	consumer, err := NewSqsConsumer(cf, consumerConfig)
	assert.NoError(t, err)

	consumerFunction := messaging.NewDefaultMessageChannelConsumeFunction(cf)
	err = consumer.Process(context.TODO(), consumerFunction)
	assert.NoError(t, err)

	waitCtx, cancelFunction := context.WithTimeout(context.Background(), 2*time.Second)
	found := false
	go func() {
	exitLabel:
		for {
			select {
			case _ = <-time.After(2 * time.Second):
				assert.Fail(t, "we should not reach here - we expected to ge ta message")
				break exitLabel

			case msg := <-consumerFunction.MessagesChannel:
				data, err := msg.PayloadAsStringObjectMap()
				assert.NoError(t, err)
				if data.StringOrEmpty("id") == id {
					found = true
					break exitLabel
				}
			}
		}

		cancelFunction()
	}()
	<-waitCtx.Done()
	assert.True(t, found, "we expect found to be set to true - it means that we got the message")
}
