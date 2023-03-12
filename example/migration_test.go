package main

import (
	"context"
	"fmt"
	goxAws "github.com/devlibx/gox-aws"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/test"
	"github.com/devlibx/gox-base/util"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/devlibx/gox-messaging/factory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestKafkaMigrationConsumeV1(t *testing.T) {
	defer goleak.VerifyNone(t)

	kafkaTopicName := os.Getenv("KAFKA_TOPIC")
	if util.IsStringEmpty(kafkaTopicName) {
		t.Skip("Need to pass kafka topic name using env var KAFKA_TOPIC")
	}

	testContext, cancelF := context.WithTimeout(context.TODO(), 20*time.Second)
	defer cancelF()
	id := uuid.NewString()
	messageCount := 5
	cf, _ := test.MockCf(t, zap.InfoLevel)
	ctx, err := goxAws.NewAwsContext(cf, goxAws.Config{})
	assert.NoError(t, err)

	consumerConfig := messaging.ConsumerConfig{
		Name:                "test",
		Type:                "kafka",
		Topic:               kafkaTopicName,
		Endpoint:            "localhost:9092",
		MigrationEnabled:    true,
		MigrationTopic:      kafkaTopicName + "_migration",
		MigrationEndpoint:   "localhost:9092",
		MigrationProperties: gox.StringObjectMap{
			// "stop_primary_after_sec": 5,
		},
		Concurrency: 4,
		Enabled:     true,
		Properties: map[string]interface{}{
			"group.id": uuid.NewString(),
			messaging.KMessagingPropertyRateLimitPerSec:         10,
			messaging.KMessagingPropertyPublishMessageTimeoutMs: 10000,
		},
		AwsContext: ctx,
	}

	producerConfig := messaging.ProducerConfig{
		Name:        "test",
		Type:        "kafka",
		Topic:       kafkaTopicName,
		Endpoint:    "localhost:9092",
		Concurrency: 1,
		Enabled:     true,
		Properties: gox.StringObjectMap{
			messaging.KMessagingPropertyPublishMessageTimeoutMs: 10000,
		},
		Async:                                  false,
		AwsContext:                             ctx,
		EnableArtificialDelayToSimulateLatency: true,
	}

	producerConfigMigration := messaging.ProducerConfig{
		Name:        "test_migration",
		Type:        "kafka",
		Topic:       kafkaTopicName + "_migration",
		Endpoint:    "localhost:9092",
		Concurrency: 1,
		Enabled:     true,
		Properties: gox.StringObjectMap{
			messaging.KMessagingPropertyPublishMessageTimeoutMs: 10000,
		},
		Async:                                  false,
		AwsContext:                             ctx,
		EnableArtificialDelayToSimulateLatency: true,
	}

	messageFactory := factory.NewMessagingFactory(cf)
	defer messageFactory.Stop()
	mgConfig := messaging.Configuration{
		Enabled: true,
		Producers: map[string]messaging.ProducerConfig{
			"test":           producerConfig,
			"test_migration": producerConfigMigration,
		},
		Consumers: map[string]messaging.ConsumerConfig{
			"test": consumerConfig,
		},
	}
	err = messageFactory.Start(mgConfig)
	assert.NoError(t, err)
	consumer, err := messageFactory.GetConsumer("test")
	assert.NoError(t, err)

	producer, err := messageFactory.GetProducer("test")
	assert.NoError(t, err)
	producerMigration, err := messageFactory.GetProducer("test_migration")
	assert.NoError(t, err)

	resultChannel := make(chan bool, 1)
	var ops int32 = 0
	go func() {
		err = consumer.Process(testContext, messaging.NewSimpleConsumeFunction(
			cf,
			"consumer_test_func",
			func(message *messaging.Message) error {
				fmt.Println("Got message... "+id+" counter=", ops, " total=", messageCount)
				p, _ := message.PayloadAsStringObjectMap()
				if p["id"] == id {
					atomic.AddInt32(&ops, 1)
					if ops >= int32(messageCount) {
						resultChannel <- true
					}
				}
				return nil
			},
			nil,
		))
	}()
	time.Sleep(6 * time.Second)

	go func() {
		time.Sleep(1 * time.Second)
		for i := 0; i < 2; i++ {
			response := <-producer.Send(testContext, &messaging.Message{
				Key:     "key",
				Payload: map[string]interface{}{"key": "value_" + id, "id": id},
			})
			assert.NoError(t, response.Err)
			if response.Err != nil {
				assert.NotNil(t, response.RawPayload)
			}
		}
	}()

	go func() {
		time.Sleep(10 * time.Second)
		for i := 0; i < messageCount-2; i++ {
			response := <-producerMigration.Send(testContext, &messaging.Message{
				Key:     "key",
				Payload: map[string]interface{}{"key": "value_" + id, "id": id},
			})
			assert.NoError(t, response.Err)
			if response.Err != nil {
				assert.NotNil(t, response.RawPayload)
			}
		}
	}()

	select {
	case <-resultChannel:
		fmt.Println("Test over - got", ops, "messages out of", messageCount)

	case <-time.After(20 * time.Second):
		assert.Fail(t, "Test failed - waited for 20 sec to get messages but did not get all messages")
	}

}
