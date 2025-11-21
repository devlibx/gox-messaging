package pubsub

import (
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/devlibx/gox-base/v2"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNewPubSubConsumer(t *testing.T) {
	projectId := os.Getenv("GOOGLE_PROJECT_ID")
	topicName := os.Getenv("GOOGLE_PUB_TOPIC")
	subscriptionName := os.Getenv("GOOGLE_PUB_SUB")
	if projectId == "" || topicName == "" || subscriptionName == "" {
		t.Skip("missing environment variables: GOOGLE_PROJECT_ID, GOOGLE_PUB_TOPIC, GOOGLE_PUB_SUB")
	}

	// Create a logger
	logger, _ := zap.NewDevelopment()

	// Create a topic for the test
	topic := setupTestTopic(t, projectId, topicName)
	defer topic.Stop()

	// Create a subscription for the test
	sub := setupTestSubscription(t, projectId, subscriptionName, topic)

	// Create a consumer config
	config := messaging.ConsumerConfig{
		Name:  "test_consumer",
		Type:  "pubsub",
		Topic: topicName,
		Properties: gox.StringObjectMap{
			"project":      projectId,
			"subscription": subscriptionName,
		},
	}

	// Create a new consumer
	consumer, err := NewPubSubConsumer(logger, config)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	// Create a consume function
	cf := gox.NewNoOpCrossFunction()
	var receivedMessage *messaging.Message
	consumeFunc := messaging.NewSimpleConsumeFunction(cf, "test-consumer", func(message *messaging.Message) error {
		receivedMessage = message
		return nil
	}, func(message *messaging.Message, err error) {
		t.Logf("error processing message: %v", err)
	})

	// Start the consumer
	err = consumer.Process(context.Background(), consumeFunc)
	assert.NoError(t, err)

	// Publish a message to the topic
	ctx, ccf := context.WithTimeout(context.Background(), 5*time.Second)
	defer ccf()
	res := topic.Publish(ctx, &pubsub.Message{
		Data: []byte("test message"),
	})
	_, err = res.Get(ctx)
	assert.NoError(t, err)

	// Wait for the message to be received
	time.Sleep(2 * time.Second)

	// Check if the message was received
	assert.NotNil(t, receivedMessage)
	assert.Equal(t, "test message", string(receivedMessage.Payload.([]byte)))

	// Stop the consumer
	err = consumer.Stop()
	assert.NoError(t, err)
}

func setupTestSubscription(t *testing.T, projectID, subID string, topic *pubsub.Topic) *pubsub.Subscription {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic: topic,
	})
	if err != nil {
		sub = client.Subscription(subID)
	}
	t.Cleanup(func() {
		sub.Delete(ctx)
		client.Close()
	})
	return sub
}
