package main

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-base"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/devlibx/gox-messaging/kafka"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"time"
)

func main() {
	zapConfig := zap.NewDevelopmentConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	cf := gox.NewCrossFunction(zapConfig.Build())

	// Send SQS message
	// err := SqsSendMessage(cf)
	err := KafkaSendMessage(cf)
	if err != nil {
		panic(err)
	}
}

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
		Properties:  gox.StringObjectMap{messaging.KMessagingPropertyPublishMessageTimeoutMs: 100000, messaging.KMessagingPropertyAcks: "1"},
	}

	producer, err := kafka.NewKafkaProducer(cf, producerConfig)
	if err != nil {
		return err
	}

	contextWithTimeout, contextCancelFunction := context.WithTimeout(context.Background(), 1*time.Second)
	defer contextCancelFunction()

	// Send a message
	id := uuid.NewString()
	response := <-producer.Send(contextWithTimeout, &messaging.Message{
		Key:     "key",
		Payload: map[string]interface{}{"key": "value", "id": id},
	})
	if response.Err != nil {
		return response.Err
	}
	fmt.Println(response.RawPayload)
	time.Sleep(time.Second)
	return nil
}
