package kafka

import (
	"flag"
	"github.com/devlibx/gox-base/test"
	"github.com/devlibx/gox-base/util"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
)

var queue string

func init() {
	var ignore string
	flag.StringVar(&queue, "real.kafka.topic", "test", "Sqs queue to ues for testing")
	flag.StringVar(&ignore, "real.sqs.queue", "", "Sqs queue to ues for testing")
}

func TestKafkaSend(t *testing.T) {
	if util.IsStringEmpty(queue) {
		t.Skip("Need to pass Kafka Queue using -real.kafka.topic=<name>")
	}
	cf, _ := test.MockCf(t, zap.InfoLevel)

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

	response, err := producer.Send(&messaging.Event{
		Key:   "key",
		Value: map[string]interface{}{"key": "value"},
	})
	assert.NoError(t, err)
	assert.NotNil(t, response)
	err = <-response.ResultChannel
	assert.NoError(t, err)
}
