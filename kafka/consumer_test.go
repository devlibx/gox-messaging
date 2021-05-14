package kafka

import (
	"context"
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

func TestKafkaConsume(t *testing.T) {
	if util.IsStringEmpty(queue) {
		t.Skip("Need to pass SQS Queue using -real.sqs.queue=<name>")
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	id := uuid.NewString()

	go func() {
		producerConfig := &messaging.ProducerConfig{
			Name:              "test",
			Type:              "kafka",
			Endpoint:          "localhost:9092",
			Topic:             queue,
			Concurrency:       1,
			Enabled:           true,
			Properties:        nil,
			Async:             false,
			DummyProducerFunc: nil,
		}

		producer, err := newKafkaProducer(cf, producerConfig)
		assert.NoError(t, err)

		for i := 0; i < 5; i++ {
			response, err := producer.Send(&messaging.Event{
				Key:   "key",
				Value: map[string]interface{}{"key": "value_" + id, "id": id},
			})
			assert.NoError(t, err)
			assert.NotNil(t, response)
			err = <-response.ResultChannel
			assert.NoError(t, err)
		}
	}()
	// time.Sleep(10 * time.Second)

	consumerConfig := &messaging.ConsumerConfig{
		Name:        "test",
		Type:        "kafka",
		Endpoint:    "localhost:9092",
		Topic:       "test",
		Concurrency: 2,
		Enabled:     true,
		Properties:  nil,
	}

	// Create consumer and listen to events
	consumer, err := newKafkaConsumer(cf, consumerConfig)
	assert.NoError(t, err)

	// Setup consumer
	contextToStopSqsSend, _ := context.WithTimeout(context.Background(), 2*time.Second)
	ackChannel := make(chan messaging.Event, 100)
	incomingEvents, err := consumer.Process(contextToStopSqsSend, ackChannel)
	assert.NoError(t, err)

	count := 0
	for event := range incomingEvents {
		if str, ok := event.Value.(string); ok {
			m := gox.StringObjectMap{}
			err := serialization.JsonBytesToObject([]byte(str), &m)
			if err == nil && m["id"] == id {
				cf.Logger().Debug("Events from Kafka", zap.Any("message", m))
				ackChannel <- event
				assert.Equal(t, "value_"+id, m["key"])
				count++
			}
		}
	}
	assert.Equal(t, 5, count)
}
