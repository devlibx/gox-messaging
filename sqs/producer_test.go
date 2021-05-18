package sqs

import (
	"context"
	"flag"
	goxAws "github.com/devlibx/gox-aws"
	"github.com/devlibx/gox-base/test"
	"github.com/devlibx/gox-base/util"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
	"time"
)

var queue string

func init() {
	var ignore string
	flag.StringVar(&queue, "real.sqs.queue", "", "Sqs queue to ues for testing")
	flag.StringVar(&ignore, "real.kafka.topic", "", "Sqs queue to ues for testing")
}

func TestSqsSendV1(t *testing.T) {
	if util.IsStringEmpty(queue) {
		t.Skip("Need to pass SQS Queue using -real.sqs.queue=<name>")
	}

	cf, _ := test.MockCf(t, zap.DebugLevel)
	ctx, err := goxAws.NewAwsContext(cf, goxAws.Config{})
	assert.NoError(t, err)

	producerConfig := messaging.ProducerConfig{
		Name:                                   "test",
		Type:                                   "sqs",
		Topic:                                  queue,
		Concurrency:                            1,
		Enabled:                                true,
		Properties:                             nil,
		Async:                                  false,
		AwsContext:                             ctx,
		EnableArtificialDelayToSimulateLatency: true,
	}

	producer, err := NewSqsProducer(cf, producerConfig)
	assert.NoError(t, err)

	// Test 1 - Test sync message send
	c, _ := context.WithTimeout(context.Background(), 1*time.Second)
	response := <-producer.Send(c, &messaging.Message{
		Key:     "key",
		Payload: map[string]interface{}{"key": "value"},
	})
	assert.NoError(t, response.Err)
	assert.NotNil(t, response.RawPayload)
	cf.Logger().Debug("Output from SQS", zap.Any("sqsResponse", response.RawPayload))

	// Test 2 - Test sync message send failed due to context timeout
	c, _ = context.WithTimeout(context.Background(), 10*time.Millisecond)
	response = <-producer.Send(c, &messaging.Message{
		Key:                              "key",
		Payload:                          map[string]interface{}{"key": "value"},
		ArtificialDelayToSimulateLatency: 100 * time.Millisecond,
	})
	assert.Error(t, response.Err)
	cf.Logger().Debug("Output from SQS", zap.Any("sqsResponse", response.Err))

	// Test 3 - do not send after producer is closed
	_ = producer.Stop()
	response = <-producer.Send(context.TODO(), &messaging.Message{
		Key:     "key",
		Payload: map[string]interface{}{"key": "value"},
	})
	assert.Error(t, response.Err)
	cf.Logger().Debug("Output from SQS", zap.Any("sqsResponse", response))
}
