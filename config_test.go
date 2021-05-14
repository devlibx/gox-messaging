package messaging

import (
	"flag"
	"github.com/devlibx/gox-base/serialization"
	"github.com/stretchr/testify/assert"
	"testing"
)

func init() {
	var ignore string
	flag.StringVar(&ignore, "real.sqs.queue", "", "Sqs queue to ues for testing")
	flag.StringVar(&ignore, "real.kafka.topic", "", "Sqs queue to ues for testing")
}

func TestConfigParsing(t *testing.T) {
	type localConfig struct {
		MessagingConfig *Configuration `yaml:"messaging"`
	}
	var lConfig = localConfig{}
	err := serialization.ReadYaml("./testdata/app.yaml", &lConfig)
	assert.NoError(t, err)

	config := lConfig.MessagingConfig
	assert.True(t, config.Enabled)
	assert.Equal(t, 2, len(config.Producers))
	assert.Equal(t, 2, len(config.Consumers))

	producer := config.Producers["client_created"]
	assert.Equal(t, "client_created_topic", producer.Topic)
	assert.Equal(t, "kafka", producer.Type)
	assert.Equal(t, "localhost:9092", producer.Endpoint)
	assert.Equal(t, true, producer.Enabled)
	assert.Equal(t, 10, producer.Concurrency)
	assert.Equal(t, "all", producer.Properties["ack"])

	producer = config.Producers["client_deleted"]
	assert.Equal(t, "client_deleted_topic", producer.Topic)
	assert.Equal(t, "kafka", producer.Type)
	assert.Equal(t, "localhost:9092", producer.Endpoint)
	assert.Equal(t, false, producer.Enabled)
	assert.Equal(t, 10, producer.Concurrency)
	assert.Equal(t, "all", producer.Properties["ack"])

	consumer := config.Consumers["client_created"]
	assert.Equal(t, "client_created_topic", consumer.Topic)
	assert.Equal(t, "kafka", consumer.Type)
	assert.Equal(t, "localhost:9092", consumer.Endpoint)
	assert.Equal(t, true, consumer.Enabled)
	assert.Equal(t, 10, consumer.Concurrency)
	assert.Equal(t, "client_created_cg", consumer.Properties["consumer_group"])
	assert.Equal(t, "all", consumer.Properties["ack"])

	consumer = config.Consumers["client_deleted"]
	assert.Equal(t, "client_deleted_topic", consumer.Topic)
	assert.Equal(t, "kafka", consumer.Type)
	assert.Equal(t, "localhost:9092", consumer.Endpoint)
	assert.Equal(t, false, consumer.Enabled)
	assert.Equal(t, 10, consumer.Concurrency)
	assert.Equal(t, "client_deleted_cg", consumer.Properties["consumer_group"])
	assert.Equal(t, "all", consumer.Properties["ack"])

}
