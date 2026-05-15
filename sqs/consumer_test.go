package sqs

import (
	"context"
	"fmt"
	goxAws "github.com/devlibx/gox-aws/v2"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/serialization"
	"github.com/devlibx/gox-base/v2/test"
	"github.com/devlibx/gox-base/v2/util"
	messaging "github.com/devlibx/gox-messaging/v2"
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
		AwsContext:                             ctx,
		EnableArtificialDelayToSimulateLatency: true,
	}

	producer, err := NewSqsProducer(cf, producerConfig)
	assert.NoError(t, err)

	id := uuid.NewString()
	messageCount := 20
	targetCount := 5

	// Setup - send messages to SQS
	go func() {
		for i := 0; i < messageCount; i++ {
			c, cf := context.WithTimeout(context.Background(), 5*time.Second)
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
		Properties: gox.StringObjectMap{
			"wait_time_seconds": 20,
		},
		AwsContext: ctx,
	}
	// Test 1 - Read message
	consumer, err := NewSqsConsumer(cf, consumerConfig)
	assert.NoError(t, err)

	done := make(chan bool)
	consumerFunc := &sqsTestConsumerFunction{
		messages:      make([]*messaging.Message, 0),
		id:            id,
		wg:            sync.WaitGroup{},
		CrossFunction: cf,
		done:          done,
		targetCount:   targetCount,
	}
	consumerFunc.wg.Add(targetCount)

	err = consumer.Process(context.Background(), consumerFunc)
	assert.NoError(t, err)

	select {
	case <-done:
		// Success
	case <-time.After(30 * time.Second):
		t.Errorf("Timeout waiting for messages")
	}

	consumerFunc.mu.Lock()
	defer consumerFunc.mu.Unlock()
	assert.GreaterOrEqual(t, len(consumerFunc.messages), targetCount)
}

func TestSqsConsumeBatchV1(t *testing.T) {

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

	id := "test_batch_" + uuid.NewString()
	messageCount := 30
	targetCount := 10

	// Setup - send messages to SQS
	fmt.Printf("Starting to send %d messages to SQS (id=%s)...\n", messageCount, id)
	wgSend := sync.WaitGroup{}
	wgSend.Add(messageCount)
	for i := 0; i < messageCount; i++ {
		go func(idx int) {
			defer wgSend.Done()
			c, cf := context.WithTimeout(context.Background(), 60*time.Second)
			_ = cf
			response := <-producer.Send(c, &messaging.Message{
				Key:     "key",
				Payload: map[string]interface{}{"key": "value_" + id, "id": id, "index": idx},
			})
			assert.NoError(t, response.Err)
			assert.NotNil(t, response.RawPayload)
		}(i)
	}
	wgSend.Wait()
	fmt.Printf("Finished sending %d messages to SQS\n", messageCount)

	consumerConfig := messaging.ConsumerConfig{
		Name:        "test",
		Type:        "sqs",
		Topic:       queue,
		Concurrency: 10,
		Enabled:     true,
		Properties: gox.StringObjectMap{
			"batch_enabled":           true,
			"max_number_of_messages": 10,
			"wait_time_seconds":      20,
			"visibility_timeout":     60,
		},
		AwsContext: ctx,
	}
	// Test 1 - Read message
	consumer, err := NewSqsBatchConsumer(cf, consumerConfig)
	assert.NoError(t, err)

	done := make(chan bool)
	consumerFunc := &sqsTestBatchConsumerFunction{
		messages:      make([]*messaging.Message, 0),
		id:            id,
		wg:            sync.WaitGroup{},
		CrossFunction: cf,
		done:          done,
		targetCount:   targetCount,
	}
	consumerFunc.wg.Add(targetCount)

	err = consumer.Process(context.Background(), consumerFunc)
	assert.NoError(t, err)

	select {
	case <-done:
		// Success
	case <-time.After(30 * time.Second):
		t.Errorf("Timeout waiting for messages (batch)")
	}


	consumerFunc.mu.Lock()
	defer consumerFunc.mu.Unlock()
	assert.GreaterOrEqual(t, len(consumerFunc.messages), targetCount)
}

type sqsTestConsumerFunction struct {
	messages    []*messaging.Message
	id          string
	wg          sync.WaitGroup
	mu          sync.Mutex
	done        chan bool
	targetCount int
	gox.CrossFunction
}

func (s *sqsTestConsumerFunction) Process(message *messaging.Message) error {
	if str, ok := message.Payload.(string); ok {
		m := gox.StringObjectMap{}
		err := serialization.JsonBytesToObject([]byte(str), &m)
		if err == nil && m["id"] == s.id {
			s.mu.Lock()
			s.messages = append(s.messages, message)
			count := len(s.messages)
			s.mu.Unlock()

			if count <= s.targetCount {
				s.wg.Done()
			}
			if count == s.targetCount {
				s.done <- true
			}
		}
	}
	return nil
}

func (s *sqsTestConsumerFunction) ErrorInProcessing(message *messaging.Message, err error) {
}

type sqsTestBatchConsumerFunction struct {
	messages    []*messaging.Message
	id          string
	wg          sync.WaitGroup
	mu          sync.Mutex
	done        chan bool
	targetCount int
	gox.CrossFunction
}

func (s *sqsTestBatchConsumerFunction) Process(message *messaging.Message) error {
	if str, ok := message.Payload.(string); ok {
		m := gox.StringObjectMap{}
		err := serialization.JsonBytesToObject([]byte(str), &m)
		if err == nil && m["id"] == s.id {
			s.mu.Lock()
			s.messages = append(s.messages, message)
			count := len(s.messages)
			s.mu.Unlock()

			if count <= s.targetCount {
				s.wg.Done()
			}
			if count%10 == 0 || count == s.targetCount {
				fmt.Printf("Consumed message %d/%d messages\n", count, s.targetCount)
			}
			if count == s.targetCount {
				s.done <- true
			}
		}
	}
	return nil
}

func (s *sqsTestBatchConsumerFunction) ErrorInProcessing(message *messaging.Message, err error) {
}
