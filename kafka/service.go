package kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/devlibx/gox-base/errors"
	"github.com/devlibx/gox-base/util"
	"time"
)

type TopicConfig struct {
	Server            string `yaml:"server"`
	Name              string `yaml:"name"`
	Partitions        int    `yaml:"partitions"`
	ReplicationFactor int    `yaml:"replication_factor"`
}

func (t *TopicConfig) SetupDefault() error {
	if util.IsStringEmpty(t.Name) {
		return errors.New("topic name is missing")
	}
	if util.IsStringEmpty(t.Server) {
		t.Server = "localhost:9092"
	}
	if t.Partitions <= 0 {
		t.Partitions = 10
	}
	if t.ReplicationFactor <= 0 {
		t.ReplicationFactor = 3
	}
	return nil
}

func CreateTopic(ctx context.Context, t *TopicConfig) (*kafka.TopicResult, error) {
	if err := t.SetupDefault(); err != nil {
		return nil, errors.Wrap(err, "failed to create a topic: name=%s", t.Name)
	}

	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": t.Server})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create a topic: name=%s", t.Name)
	}

	results, err := admin.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{
			{
				Topic:             t.Name,
				NumPartitions:     t.Partitions,
				ReplicationFactor: t.ReplicationFactor},
		},
		kafka.SetAdminOperationTimeout(5*time.Second),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create a topic: name=%s", t.Name)
	}
	return &results[0], nil
}
