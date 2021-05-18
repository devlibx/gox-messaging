package kafka

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
	"go.uber.org/goleak"
	"go.uber.org/zap"
	context2 "golang.org/x/net/context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKafkaConsumeV1(t *testing.T) {
	defer goleak.VerifyNone(t)

	if util.IsStringEmpty(queue) {
		t.Skip("Need to pass SQS Queue using -real.kafka.topic=<name>")
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	ctx, err := goxAws.NewAwsContext(cf, goxAws.Config{})
	assert.NoError(t, err)

	producerConfig := messaging.ProducerConfig{
		Name:                                   "test",
		Type:                                   "kafka",
		Topic:                                  queue,
		Endpoint:                               "localhost:9092",
		Concurrency:                            1,
		Enabled:                                true,
		Properties:                             nil,
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
		Topic:       queue,
		Endpoint:    "localhost:9092",
		Concurrency: 2,
		Enabled:     true,
		// Properties: map[string]interface{}{"group.id": "1234"},
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
			atomic.AddInt32(&ops, 1)
			if ops >= int32(messageCount) {
				resultChannel <- true
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
		assert.Fail(t, "failed with timeout - we expected to get messages recieved in consumer from kafka")
		return
	case <-resultChannel:
		// No Op
	}
	time.Sleep(2 * time.Second)
	producer.Stop()
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
