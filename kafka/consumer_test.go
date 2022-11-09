package kafka

import (
	"context"
	"fmt"
	goxAws "github.com/devlibx/gox-aws"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/serialization"
	"github.com/devlibx/gox-base/test"
	"github.com/devlibx/gox-base/util"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	context2 "golang.org/x/net/context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKafkaConsumeV1(t *testing.T) {
	defer goleak.VerifyNone(t)

	kafkaTopicName := os.Getenv("KAFKA_TOPIC")
	if util.IsStringEmpty(kafkaTopicName) {
		t.Skip("Need to pass kafka topic name using env var KAFKA_TOPIC")
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	ctx, err := goxAws.NewAwsContext(cf, goxAws.Config{})
	assert.NoError(t, err)

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

	producer, err := NewKafkaProducer(cf, producerConfig)
	assert.NoError(t, err)

	id := uuid.NewString()
	messageCount := 5

	// Setup - send 5 messages to SQS
	go func() {
		time.Sleep(2 * time.Second)
		for i := 0; i < messageCount; i++ {
			c, cf := context.WithTimeout(context.Background(), 1*time.Second)
			_ = cf
			response := <-producer.Send(c, &messaging.Message{
				Key:     "key",
				Payload: map[string]interface{}{"key": "value_" + id, "id": id},
			})
			assert.NoError(t, response.Err)
			if response.Err != nil {
				assert.NotNil(t, response.RawPayload)
			}
		}
	}()

	consumerConfig := messaging.ConsumerConfig{
		Name:        "test",
		Type:        "kafka",
		Topic:       kafkaTopicName,
		Endpoint:    "localhost:9092",
		Concurrency: 2,
		Enabled:     true,
		Properties: map[string]interface{}{
			"group.id": uuid.NewString(),
			messaging.KMessagingPropertyRateLimitPerSec:         10,
			messaging.KMessagingPropertyPublishMessageTimeoutMs: 10000,
		},
		AwsContext: ctx,
	}
	// Test 1 - Read message
	cf.Logger().Info("Start kafka consumer")
	consumer, err := NewKafkaConsumer(cf, consumerConfig)
	assert.NoError(t, err)

	consumerFunc := &sqsTestConsumerFunction{
		messages:      make([]*messaging.Message, 0),
		id:            id,
		wg:            sync.WaitGroup{},
		CrossFunction: cf,
	}
	consumerFunc.wg.Add(messageCount)

	ctxx, ctxxCancel := context2.WithCancel(context.TODO())
	defer ctxxCancel()
	resultChannel := make(chan bool, 1)
	var ops int32 = 0
	err = consumer.Process(ctxx, messaging.NewSimpleConsumeFunction(
		cf,
		"consumer_test_func",
		func(message *messaging.Message) error {
			fmt.Println("Got message... " + id)
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
	assert.NoError(t, err)

	select {
	case <-time.After(5 * time.Second):
		producer.Stop()
		time.Sleep(2 * time.Second)
		assert.Fail(t, "failed with timeout - we expected to get messages received in consumer from kafka")
		return
	case <-resultChannel:
		// No Op
	}
	time.Sleep(2 * time.Second)
	producer.Stop()
	assert.Equal(t, int32(messageCount), ops)
}

func TestKafkaConsumeV1WithConcurrency(t *testing.T) {
	defer goleak.VerifyNone(t)

	kafkaTopicName := os.Getenv("KAFKA_TOPIC")
	if util.IsStringEmpty(kafkaTopicName) {
		t.Skip("Need to pass kafka topic name using env var KAFKA_TOPIC")
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	ctx, err := goxAws.NewAwsContext(cf, goxAws.Config{})
	assert.NoError(t, err)

	producerConfig := messaging.ProducerConfig{
		Name:        "test",
		Type:        "kafka",
		Topic:       kafkaTopicName,
		Endpoint:    "localhost:9092",
		Concurrency: 1,
		Enabled:     true,
		Properties: gox.StringObjectMap{
			messaging.KMessagingPropertyPublishMessageTimeoutMs: 1000,
		},
		Async:                                  false,
		AwsContext:                             ctx,
		EnableArtificialDelayToSimulateLatency: true,
	}

	producer, err := NewKafkaProducer(cf, producerConfig)
	assert.NoError(t, err)

	id := uuid.NewString()
	messageCount := 20

	// Setup - send 5 messages to SQS
	go func() {
		time.Sleep(2 * time.Second)
		for i := 0; i < messageCount; i++ {
			c, _ := context.WithTimeout(context.Background(), 2*time.Second)
			response := <-producer.Send(c, &messaging.Message{
				Key:     fmt.Sprintf("key_%d", i%10),
				Payload: map[string]interface{}{"key": "value_" + id, "id": id},
			})
			assert.NoError(t, response.Err)
			if response.Err != nil {
				assert.NotNil(t, response.RawPayload)
			}
		}
	}()

	consumerConfig := messaging.ConsumerConfig{
		Name:        "test",
		Type:        "kafka",
		Topic:       kafkaTopicName,
		Endpoint:    "localhost:9092",
		Concurrency: 2,
		Enabled:     true,
		Properties: map[string]interface{}{
			"group.id": uuid.NewString(),
			messaging.KMessagingPropertyRateLimitPerSec:                1000,
			messaging.KMessagingPropertyPartitionProcessingParallelism: 5,
		},
		AwsContext: ctx,
	}
	// Test 1 - Read message
	cf.Logger().Info("Start kafka consumer")
	consumer, err := NewKafkaConsumer(cf, consumerConfig)
	assert.NoError(t, err)

	consumerFunc := &sqsTestConsumerFunction{
		messages:      make([]*messaging.Message, 0),
		id:            id,
		wg:            sync.WaitGroup{},
		CrossFunction: cf,
	}
	consumerFunc.wg.Add(messageCount)

	ctxx, ctxxCancel := context2.WithCancel(context.TODO())
	defer ctxxCancel()
	resultChannel := make(chan bool, 1)
	var ops int32 = 0
	err = consumer.Process(ctxx, messaging.NewSimpleConsumeFunction(
		cf,
		"consumer_test_func",
		func(message *messaging.Message) error {
			fmt.Println("Got message... key={}", message.Key)
			time.Sleep(2 * time.Second)
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
	assert.NoError(t, err)

	select {
	case <-time.After(1 * time.Minute):
		_ = producer.Stop()
		time.Sleep(2 * time.Second)
		assert.Fail(t, "failed with timeout - we expected to get messages received in consumer from kafka")
		return
	case <-resultChannel:
		// No Op
	}
	time.Sleep(2 * time.Second)
	_ = producer.Stop()
	assert.Equal(t, int32(messageCount), ops)
}

type sqsTestConsumerFunction struct {
	messages []*messaging.Message
	id       string
	wg       sync.WaitGroup
	gox.CrossFunction
}

func (s *sqsTestConsumerFunction) Process(message *messaging.Message) error {
	if str, ok := message.Payload.([]byte); ok {
		m := gox.StringObjectMap{}
		err := serialization.JsonBytesToObject([]byte(str), &m)
		if err == nil && m["id"] == s.id {
			s.messages = append(s.messages, message)
			s.wg.Done()
		}
	}
	return nil
}

func (s *sqsTestConsumerFunction) ErrorInProcessing(message *messaging.Message, err error) {
	panic("implement me")
}

func TestStringToHashMod(t *testing.T) {
	for i := 0; i < 10000; i++ {
		id := StringToHashMod(uuid.NewString(), 10)
		assert.True(t, id >= 0)
		assert.True(t, id < 10)
	}

	for i := 0; i < 10000; i++ {
		id := StringToHashMod(uuid.NewString(), 1)
		assert.True(t, id >= 0)
		assert.True(t, id < 1)
	}
}
