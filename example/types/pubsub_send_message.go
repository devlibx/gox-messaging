package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/devlibx/gox-base/v2"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/devlibx/gox-messaging/v2/pubsub"
	"github.com/google/uuid"
)

func PubSubSendMessage(cf gox.CrossFunction) error {

	projectId := os.Getenv("GOOGLE_PROJECT_ID")
	topicName := os.Getenv("GOOGLE_PUB_TOPIC")
	subscriptionName := os.Getenv("GOOGLE_PUB_SUB")

	if projectId == "" || topicName == "" || subscriptionName == "" {
		return fmt.Errorf("missing environment variables: GOOGLE_PROJECT_ID, GOOGLE_PUB_TOPIC, GOOGLE_PUB_SUB")
	}

	// Setup 1 - create a producer
	producerConfig := messaging.ProducerConfig{
		Name:        "my-pubsub-topic",
		Type:        "pubsub",
		Topic:       topicName,
		Concurrency: 1,
		Enabled:     true,
		Properties: gox.StringObjectMap{
			"project": projectId,
		},
	}

	producer, err := pubsub.NewPubSubProducer(cf.Logger(), producerConfig)
	if err != nil {
		return err
	}

	// Send a message
	id := uuid.NewString()
	response := <-producer.Send(context.Background(), &messaging.Message{
		Key:     "key-harish-",
		Payload: map[string]interface{}{"key": "value", "id": id, "time": time.Now().String()},
	})
	if response.Err != nil {
		return response.Err
	}
	fmt.Println("Sent message with ID:", response.RawPayload)

	// Setup 2 - create a consumer
	consumerConfig := messaging.ConsumerConfig{
		Name:        "my-pubsub-topic",
		Type:        "pubsub",
		Topic:       topicName,
		Concurrency: 1,
		Enabled:     true,
		Properties: gox.StringObjectMap{
			"project":      projectId,
			"subscription": subscriptionName,
		},
	}
	consumer, err := pubsub.NewPubSubConsumer(cf.Logger(), consumerConfig)
	if err != nil {
		return err
	}

	// Start consumer
	err = consumer.Process(context.Background(), messaging.NewSimpleConsumeFunction(cf, "my-consumer",
		func(message *messaging.Message) error {
			fmt.Println("Received message:", message.Payload)
			return nil
		},
		func(message *messaging.Message, err error) {
			fmt.Println("Error processing message:", err)
		},
	))
	if err != nil {
		return err
	}

	// Wait for a message to be received
	time.Sleep(10 * time.Second)

	return nil
}
