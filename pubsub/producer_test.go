package pubsub

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/devlibx/gox-base/v2"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNewPubSubProducer(t *testing.T) {
	projectId := strings.TrimSpace(os.Getenv("GOOGLE_PROJECT_ID"))
	topicName := strings.TrimSpace(os.Getenv("GOOGLE_PUB_TOPIC"))
	if projectId == "" || topicName == "" {
		t.Skip("missing environment variables: GOOGLE_PROJECT_ID, GOOGLE_PUB_TOPIC")
	}

	// Create a logger
	logger, _ := zap.NewDevelopment()

	// Create a producer config
	config := messaging.ProducerConfig{
		Name:  "test_producer",
		Type:  "pubsub",
		Topic: topicName,
		Properties: gox.StringObjectMap{
			"project": projectId,
		},
	}

	// Create a new producer
	producer, err := NewPubSubProducer(logger, config)
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	// Send a message
	ctx, cf := context.WithTimeout(context.Background(), 5*time.Second)
	defer cf()
	ch := producer.Send(ctx, &messaging.Message{
		Payload: "test message",
	})

	// Wait for the response
	select {
	case resp := <-ch:
		assert.NoError(t, resp.Err)
		assert.NotNil(t, resp.RawPayload)
	case <-ctx.Done():
		t.Fatal("timed out waiting for message to be sent")
	}

	// Stop the producer
	err = producer.Stop()
	assert.NoError(t, err)
}

func setupTestTopic(t *testing.T, projectID, topicID string) *pubsub.Topic {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		topic = client.Topic(topicID)
	}
	t.Cleanup(func() {
		topic.Delete(ctx)
		client.Close()
	})
	return topic
}
