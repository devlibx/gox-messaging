package factory

import (
	"context"
	"flag"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/serialization"
	"github.com/devlibx/gox-base/v2/test"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"testing"
	"time"
)

func init() {
	var ignore string
	flag.StringVar(&ignore, "real.kafka.topic", "", "Sqs queue to ues for testing")
	flag.StringVar(&ignore, "real.sqs.queue", "", "Sqs queue to ues for testing")
}

type dummyPcDcConfigs struct {
	Messaging messaging.Configuration `yaml:"messaging"`
}

type dummyPcDcConfigs1 messaging.Configuration

func TestDummyQueue(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Setup producers
	cf, _ := test.MockCf(t, zap.InfoLevel)
	service := NewMessagingFactory(cf)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	configFromFile := dummyPcDcConfigs{}
	err := serialization.ReadYamlFromString(yamlF, &configFromFile)
	assert.NoError(t, err)

	err = service.Start(configFromFile.Messaging)
	assert.NoError(t, err)

	producerConfig := configFromFile.Messaging.Producers["internal_kafka_topic"]
	producerConfig.Name = "internal_kafka_topic"
	producerConfig.PopulateWithStringObjectMap(gox.StringObjectMap{})
	err = service.RegisterProducer(producerConfig)
	assert.NoError(t, err)

	// Setup consumer and start it - for test purpose we want o make sure we are done with kafka topic
	// assignment - otherwise we miss event in testing
	consumerConfig := producerConfig.BuildConsumerConfig()
	err = service.RegisterConsumer(consumerConfig)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	// Get consumer and start consuming
	consumerFunction := messaging.NewDefaultMessageChannelConsumeFunction(cf)
	consumer, err := service.GetConsumer("internal_kafka_topic")
	assert.NoError(t, err)
	err = consumer.Process(ctx, consumerFunction)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	// Close channel forcefully to stop this test
	go func() {
		select {
		case <-ctx.Done():
			close(consumerFunction.MessagesChannel)
			service.Stop()
		}
	}()

	producer, err := service.GetProducer("internal_kafka_topic")
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		responseChannel := <-producer.Send(ctx, &messaging.Message{
			Key:     "test",
			Payload: map[string]interface{}{"key": "value"},
		})
		assert.NoError(t, responseChannel.Err)
	}

	gotEvents := false
	for eventFromConsumer := range consumerFunction.MessagesChannel {
		data, _ := eventFromConsumer.PayloadAsStringObjectMap()
		cf.Logger().Info("message from consumer", zap.Any("message", data))
		gotEvents = true
		break
	}
	assert.True(t, gotEvents)
}

var yamlF = `
messaging:
  enabled: true
  producers:
    internal_kafka_topic:
      type: dummy
      topic: dummy
      concurrency: 1
      enabled: true

`
