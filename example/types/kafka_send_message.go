package main

import (
	"context"
	"fmt"
	"time"

	"github.com/devlibx/gox-base/v2"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/devlibx/gox-messaging/v2/kafka"
	"github.com/google/uuid"
	context2 "golang.org/x/net/context"
	"sync/atomic"
)

func KafkaSendMessage(cf gox.CrossFunction) error {

	// Setup 1 - create a producer
	producerConfig := messaging.ProducerConfig{
		Name:        "test_queue",
		Type:        "kafka",
		Endpoint:    "localhost:9092",
		Topic:       "harish_test",
		Concurrency: 1,
		Enabled:     true,
		Async:       false,
		Properties: gox.StringObjectMap{
			messaging.KMessagingPropertyPublishMessageTimeoutMs:   100,
			messaging.KMessagingPropertyAcks:                      "1",
			messaging.KMessagingPropertyErrorReportingChannelSize: 100,
		},
		KafkaSpecificProperty: map[string]interface{}{
			"acks":             "0",
			"compression.type": "gzip",
		},
	}

	producer, err := kafka.NewKafkaProducer(cf, producerConfig)
	if err != nil {
		return err
	}

	if r, ok := producer.(messaging.ErrorReporter); ok {
		fmt.Println("")
		go func() {
			if ch, enabled, err := r.GetErrorReport(); err == nil && enabled {
				for errorR := range ch {
					fmt.Println(errorR.Err, errorR.RawPayload)
				}
			}
		}()
	}

	contextWithTimeout, contextCancelFunction := context.WithTimeout(context.Background(), 100*time.Second)
	defer contextCancelFunction()

	go func() {
		KafkaReadMessage(cf, "harish_test")
	}()

	// Send a message
	for {
		id := uuid.NewString()
		response := <-producer.Send(contextWithTimeout, &messaging.Message{
			Key:     "key",
			Payload: map[string]interface{}{"key": "value", "id": id},
		})
		if response.Err != nil {
			fmt.Println("Error =>>>>", response.Err)
		} else {
			//	fmt.Println(response.RawPayload)
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(time.Second)
	return nil
}

func KafkaReadMessage(cf gox.CrossFunction, kafkaTopicName string) {
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
		KafkaSpecificProperty: map[string]interface{}{
			"acks":             "0",
			"compression.type": "gzip",
		},
	}
	// Test 1 - Read message
	cf.Logger().Info("Start kafka consumer")
	consumer, err := kafka.NewKafkaConsumer(cf, consumerConfig)
	if err != nil {
		panic(err)
	}

	var count int32 = 0
	ctx, ctxCancel := context2.WithCancel(context.TODO())
	defer ctxCancel()
	err = consumer.Process(ctx, messaging.NewSimpleConsumeFunction(
		cf,
		"consumer_test_func",
		func(message *messaging.Message) error {
			atomic.AddInt32(&count, 1)
			m, err := message.PayloadAsStringObjectMap()
			fmt.Println("Got message... ", m, err, "message_count=", count)
			return nil
		},
		nil,
	))
	time.Sleep(1 * time.Hour)
}
