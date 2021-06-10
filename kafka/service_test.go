package kafka

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var runTests = false

func TestCreateTopic(t *testing.T) {
	if !runTests {
		t.Skip("do not run this test - this needs a running kafka")
	}
	topicName := "dummy_" + uuid.NewString()
	tc := &TopicConfig{
		Server:            "localhost:9092",
		Name:              topicName,
		Partitions:        3,
		ReplicationFactor: 1,
	}
	fmt.Println("kafka-topics --bootstrap-server localhost:9092 --list")
	fmt.Println("kafka-topics --bootstrap-server localhost:9092 --describe --topic " + topicName)

	ctx, cn := context.WithTimeout(context.Background(), 5*time.Second)
	defer cn()
	result, err := CreateTopic(ctx, tc)
	assert.NoError(t, err)
	fmt.Println(result.Error)
}

func TestCreateTopicWithBadServer(t *testing.T) {
	if !runTests {
		t.Skip("do not run this test - this needs a running kafka")
	}
	topicName := "dummy_" + uuid.NewString()
	tc := &TopicConfig{
		Server:            "localhost:90921",
		Name:              topicName,
		Partitions:        3,
		ReplicationFactor: 1,
	}
	fmt.Println("kafka-topics --bootstrap-server localhost:9092 --list")
	fmt.Println("kafka-topics --bootstrap-server localhost:9092 --describe --topic " + topicName)

	ctx, cn := context.WithTimeout(context.Background(), 5*time.Second)
	defer cn()
	result, err := CreateTopic(ctx, tc)
	assert.Error(t, err)
	_ = result
}
