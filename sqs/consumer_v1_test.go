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
	"sync"
	"testing"
	"time"
)

func TestSqsConsumeV1(t *testing.T) {

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
		DummyProducerFunc:                      nil,
		AwsContext:                             ctx,
		EnableArtificialDelayToSimulateLatency: true,
	}

	producer, err := NewSqsProducerV1(cf, producerConfig)
	assert.NoError(t, err)

	id := uuid.NewString()
	messageCount := 5

	// Setup - send 5 messages to SQS
	go func() {
		for i := 0; i < messageCount; i++ {
			c, cf := context.WithTimeout(context.Background(), 1*time.Second)
			_ = cf
			response := <-producer.Send(c, &messaging.Message{
				Key:     "key",
				Payload: map[string]interface{}{"key": "value_" + id, "id": id},
			})
			assert.NoError(t, response.Err)
			assert.NotNil(t, response.RawPayload)
		}
	}()

	consumerConfig := messaging.ConsumerConfig{
		Name:        "test",
		Type:        "sqs",
		Topic:       queue,
		Concurrency: 2,
		Enabled:     true,
		Properties:  nil,
		AwsContext:  ctx,
	}
	// Test 1 - Read message
	consumer, err := NewSqsConsumerV1(cf, consumerConfig)
	assert.NoError(t, err)

	consumerFunc := &sqsTestConsumerFunction{
		messages:      make([]*messaging.Message, 0),
		id:            id,
		wg:            sync.WaitGroup{},
		CrossFunction: cf,
	}
	consumerFunc.wg.Add(messageCount)

	err = consumer.Process(context.Background(), consumerFunc)
	assert.NoError(t, err)

	// If test does not finish then complete it 5 sec
	go func() {
		time.Sleep(20 * time.Second)
		for i := 0; i < messageCount; i++ {
			consumerFunc.wg.Done()
		}
	}()
	consumerFunc.wg.Wait()

	assert.Equal(t, messageCount, len(consumerFunc.messages))
}

type sqsTestConsumerFunction struct {
	messages []*messaging.Message
	id       string
	wg       sync.WaitGroup
	gox.CrossFunction
}

func (s *sqsTestConsumerFunction) Process(message *messaging.Message) error {
	if str, ok := message.Payload.(string); ok {
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
