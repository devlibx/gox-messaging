package sqs

import (
	"context"
	goxAws "github.com/devlibx/gox-aws"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/serialization"
	"github.com/devlibx/gox-base/test"
	"github.com/devlibx/gox-base/util"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestSqsConsume(t *testing.T) {
	if util.IsStringEmpty(queue) {
		t.Skip("Need to pass SQS Queue using -real.sqs.queue=<name>")
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	ctx, err := goxAws.NewAwsContext(cf, goxAws.Config{})
	assert.NoError(t, err)

	id := uuid.NewString()

	go func() {
		// Send event to test
		producer := NewSqsProducer(cf, ctx, Config{QueueUrl: queue})

		for i := 0; i < 5; i++ {
			response, err := producer.Send(&messaging.Event{
				Key:   "key",
				Value: map[string]interface{}{"key": "value_" + id, "id": id},
			})
			assert.NoError(t, err)
			assert.NotNil(t, response)
		}
	}()

	// Create consumer and listen to events
	consumer := NewSqsConsumer(cf, ctx, Config{QueueUrl: queue})

	// Setup consumer
	contextToStopSqsSend, _ := context.WithTimeout(context.Background(), 10*time.Second)
	ackChannel := make(chan messaging.Event, 100)
	incomingEvents, err := consumer.Process(contextToStopSqsSend, ackChannel)
	assert.NoError(t, err)

	count := 0
	for event := range incomingEvents {
		if str, ok := event.Value.(string); ok {
			m := gox.StringObjectMap{}
			err := serialization.JsonBytesToObject([]byte(str), &m)
			assert.NoError(t, err)
			if m["id"] == id {
				cf.Logger().Debug("Events from SQS", zap.Any("message", m))
				ackChannel <- event
				assert.Equal(t, "value_"+id, m["key"])
				count++
			}
		}
	}
	// assert.Equal(t, 5, count)
	assert.True(t, count > 0)
}
