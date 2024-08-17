package messaging

import (
	"github.com/devlibx/gox-base/v2"
	errors2 "github.com/devlibx/gox-base/v2/errors"
	"strings"

	goxAws "github.com/devlibx/gox-aws/v2"
	"github.com/devlibx/gox-base/v2/util"
	"github.com/google/uuid"
)

type DummyProducerFunc func(key string, value []byte) error

type ProducerConfig struct {
	Name                                   string
	Type                                   string                 `yaml:"type" json:"type"`
	Endpoint                               string                 `yaml:"endpoint" json:"endpoint"`
	Topic                                  string                 `yaml:"topic" json:"topic"`
	Concurrency                            int                    `yaml:"concurrency" json:"concurrency"`
	Enabled                                bool                   `yaml:"enabled" json:"enabled"`
	Properties                             map[string]interface{} `yaml:"properties" json:"properties"`
	Async                                  bool                   `yaml:"async" json:"async"`
	MessageTimeoutInMs                     int                    `yaml:"message_timeout_ms" json:"message_timeout_ms"`
	EnableArtificialDelayToSimulateLatency bool                   `yaml:"enable_artificial_delay_to_simulate_latency" json:"enable_artificial_delay_to_simulate_latency"`
	MaxMessageInBuffer                     int                    `yaml:"max_message_in_buffer" json:"max_message_in_buffer"`
	AwsConfig                              goxAws.Config          `yaml:"aws" json:"aws"`
	KafkaSpecificProperty                  map[string]interface{} `yaml:"kafka_specific_property" json:"kafka_specific_property"`
	AwsContext                             goxAws.AwsContext
}

type ConsumerConfig struct {
	Name                  string
	Type                  string                 `yaml:"type" json:"type"`
	Endpoint              string                 `yaml:"endpoint" json:"endpoint"`
	Topic                 string                 `yaml:"topic" json:"topic"`
	Concurrency           int                    `yaml:"concurrency" json:"concurrency"`
	Enabled               bool                   `yaml:"enabled" json:"enabled"`
	Properties            gox.StringObjectMap    `yaml:"properties" json:"properties"`
	KafkaSpecificProperty map[string]interface{} `yaml:"kafka_specific_property" json:"kafka_specific_property"`
	AwsConfig             goxAws.Config          `yaml:"aws" json:"aws"`
	AwsContext            goxAws.AwsContext

	// These are used for migration
	MigrationEnabled    bool                `yaml:"migration_enabled" json:"migration_enabled"`
	MigrationEndpoint   string              `yaml:"migration_endpoint" json:"migration_endpoint"`
	MigrationTopic      string              `yaml:"migration_topic" json:"migration_topic"`
	MigrationProperties gox.StringObjectMap `yaml:"migration_properties" json:"migration_properties"`
}

type Configuration struct {
	Enabled   bool                      `yaml:"enabled" json:"enabled"`
	Producers map[string]ProducerConfig `yaml:"producers" json:"producers"`
	Consumers map[string]ConsumerConfig `yaml:"consumers" json:"consumers"`
}

func (p *ProducerConfig) SetupDefaults() {
	if p.Properties == nil {
		p.Properties = map[string]interface{}{}
	}
	if v, ok := p.Properties["acks"].(int); ok {
		p.Properties["acks"] = v
	} else if _, ok := p.Properties["acks"].(string); !ok {
		p.Properties["acks"] = "all"
	}
	if p.MessageTimeoutInMs <= 0 {
		if val, ok := p.Properties[KMessagingPropertyPublishMessageTimeoutMs].(int); ok {
			p.MessageTimeoutInMs = val
		} else {
			p.MessageTimeoutInMs = 20
		}
	}
	if p.MaxMessageInBuffer <= 0 {
		p.MaxMessageInBuffer = 1000
	}
	if _, ok := p.Properties[KMessagingPropertyDisableDeliveryReports].(bool); !ok {
		p.Properties[KMessagingPropertyDisableDeliveryReports] = true
	}
	if p.KafkaSpecificProperty == nil {
		p.KafkaSpecificProperty = map[string]interface{}{}
	}
}

func (p *ConsumerConfig) SetupDefaults() {
	if p.Properties == nil {
		p.Properties = map[string]interface{}{}
	}
	if _, ok := p.Properties["group.id"].(string); !ok {
		p.Properties["group.id"] = uuid.NewString()
	}
	if _, ok := p.Properties["auto.offset.reset"].(string); !ok {
		p.Properties["auto.offset.reset"] = "latest"
	}
	if _, ok := p.Properties[KMessagingPropertyEnableAutoCommit].(string); !ok {
		p.Properties[KMessagingPropertyEnableAutoCommit] = "true"
	}
	if p.Concurrency <= 0 {
		p.Concurrency = 1
	}
}

func (p *ConsumerConfig) PopulateWithStringObjectMap(input gox.StringObjectMap) {
	if strings.ToLower(p.Type) == "kafka" {
		if util.IsStringEmpty(p.Endpoint) {
			p.Endpoint = input.StringOrDefault(KMessagingPropertyEndpoint, "localhost:9092")
		}
		if util.IsStringEmpty(p.Topic) {
			p.Topic = input.StringOrDefault(KMessagingPropertyTopic, "test")
		}
		if p.Concurrency <= 0 {
			p.Concurrency = input.IntOrDefault(KMessagingPropertyConcurrency, 1)
		}

		// Set migration properties
		if _migrationEnabled, ok := input[KMessagingPropertyMigrationEnabled]; ok {
			if migrationEnabled, ok := _migrationEnabled.(bool); ok && migrationEnabled {
				p.MigrationEnabled = true
				p.MigrationTopic = input.StringOrDefault(KMessagingPropertyMigrationTopic, "test_migration")
				p.MigrationEndpoint = input.StringOrDefault(KMessagingPropertyMigrationEndpoint, "localhost:9092")
			}
		}

		if p.Properties == nil {
			p.Properties = map[string]interface{}{}
		}
		if _, ok := p.Properties["group.id"]; !ok {
			p.Properties["group.id"] = input.StringOrDefault(KMessagingPropertyGroupId, "groupId")
		}
		if _, ok := p.Properties["auto.offset.reset"]; !ok {
			p.Properties["auto.offset.reset"] = input.StringOrDefault(KMessagingPropertyAutoOffsetReset, "latest")
		}
		if _, ok := p.Properties["enable.auto.commit"]; !ok {
			p.Properties["enable.auto.commit"] = input.StringOrDefault(KMessagingPropertyEnableAutoCommit, "true")
		}
		if _, ok := p.Properties["log_no_message"]; !ok {
			p.Properties["log_no_message"] = input.BoolOrFalse("log_no_message")
		}
		if _, ok := p.Properties["log_no_message_mod"]; !ok {
			p.Properties["log_no_message_mod"] = input.IntOrDefault("log_no_message_mod", 10)
		}
		if _, ok := p.Properties["session.timeout.ms"]; !ok {
			p.Properties["session.timeout.ms"] = input.IntOrDefault(KMessagingPropertySessionTimeoutMs, 10000)
		}
		if _, ok := p.Properties[KMessagingPropertyRateLimitPerSec]; !ok {
			p.Properties[KMessagingPropertyRateLimitPerSec] = input.IntOrDefault(KMessagingPropertyRateLimitPerSec, 0)
		}
		if _, ok := p.Properties[KMessagingPropertyPartitionProcessingParallelism]; !ok {
			p.Properties[KMessagingPropertyPartitionProcessingParallelism] = input.IntOrDefault(KMessagingPropertyPartitionProcessingParallelism, 0)
		}
	} else if strings.ToLower(p.Type) == "dummy" {
		if util.IsStringEmpty(p.Endpoint) {
			p.Endpoint = input.StringOrDefault(KMessagingPropertyEndpoint, "localhost:9092")
		}
		if util.IsStringEmpty(p.Topic) {
			p.Topic = input.StringOrDefault(KMessagingPropertyTopic, "test")
		}
		if p.Concurrency <= 0 {
			p.Concurrency = input.IntOrDefault(KMessagingPropertyConcurrency, 1)
		}
		if p.Properties == nil {
			p.Properties = map[string]interface{}{}
		}
		if _, ok := p.Properties["group.id"]; !ok {
			p.Properties["group.id"] = input.StringOrDefault(KMessagingPropertyGroupId, "groupId")
		}
		if _, ok := p.Properties["group.id"]; !ok {
			p.Properties["auto.offset.reset"] = input.StringOrDefault(KMessagingPropertyAutoOffsetReset, "latest")
		}
	} else if strings.ToLower(p.Type) == "sqs" {
		if util.IsStringEmpty(p.Topic) {
			p.Topic = input.StringOrDefault(KMessagingPropertyTopic, "test")
		}
	}
}

func (p *ProducerConfig) PopulateWithStringObjectMap(input gox.StringObjectMap) {
	if strings.ToLower(p.Type) == "kafka" {
		if util.IsStringEmpty(p.Endpoint) {
			p.Endpoint = input.StringOrDefault(KMessagingPropertyEndpoint, "localhost:9092")
		}
		if util.IsStringEmpty(p.Topic) {
			p.Topic = input.StringOrDefault(KMessagingPropertyTopic, "test")
		}
		if p.Concurrency <= 0 {
			p.Concurrency = input.IntOrDefault(KMessagingPropertyConcurrency, 1)
		}
		if p.Properties == nil {
			p.Properties = map[string]interface{}{}
		}
		if _, ok := p.Properties["acks"].(string); !ok {
			p.Properties["acks"] = input.StringOrDefault(KMessagingPropertyAcks, "all")
		}
		if _, ok := p.Properties[KMessagingPropertyPublishMessageTimeoutMs]; !ok {
			p.Properties[KMessagingPropertyPublishMessageTimeoutMs] = input.IntOrDefault(KMessagingPropertyPublishMessageTimeoutMs, 20)
		}
	} else if strings.ToLower(p.Type) == "dummy" {
		if util.IsStringEmpty(p.Endpoint) {
			p.Endpoint = input.StringOrDefault(KMessagingPropertyEndpoint, "localhost:9092")
		}
		if util.IsStringEmpty(p.Topic) {
			p.Topic = input.StringOrDefault(KMessagingPropertyTopic, "test")
		}
		if p.Concurrency <= 0 {
			p.Concurrency = input.IntOrDefault(KMessagingPropertyConcurrency, 1)
		}
		if p.Properties == nil {
			p.Properties = map[string]interface{}{}
		}
		if _, ok := p.Properties["acks"].(string); !ok {
			p.Properties["acks"] = input.StringOrDefault(KMessagingPropertyAcks, "all")
		}
	} else if strings.ToLower(p.Type) == "sqs" {
		if util.IsStringEmpty(p.Topic) {
			p.Topic = input.StringOrDefault(KMessagingPropertyTopic, "test")
		}
	}
}

func (p *ProducerConfig) BuildConsumerConfig() ConsumerConfig {
	config := ConsumerConfig{}
	if strings.ToLower(p.Type) == "kafka" {
		config.Type = p.Type
		config.Endpoint = p.Endpoint
		config.Topic = p.Topic
		config.Name = p.Name
		config.Concurrency = p.Concurrency
		config.Enabled = true
		config.SetupDefaults()
	} else if strings.ToLower(p.Type) == "dummy" {
		config.Type = p.Type
		config.Endpoint = p.Endpoint
		config.Topic = p.Topic
		config.Name = p.Name
		config.Concurrency = p.Concurrency
		config.Enabled = true
		config.SetupDefaults()
	}
	return config
}

func (p *ConsumerConfig) BuildProducerConfig() ProducerConfig {
	config := ProducerConfig{}
	if strings.ToLower(p.Type) == "kafka" {
		config.Type = p.Type
		config.Endpoint = p.Endpoint
		config.Topic = p.Topic
		config.Name = p.Name
		config.Concurrency = p.Concurrency
		config.Enabled = true
		config.SetupDefaults()
	} else if strings.ToLower(p.Type) == "dummy" {
		config.Type = p.Type
		config.Endpoint = p.Endpoint
		config.Topic = p.Topic
		config.Name = p.Name
		config.Concurrency = p.Concurrency
		config.Enabled = true
		config.SetupDefaults()
	}
	return config
}

func (p *ConsumerConfig) IsMigrationEnabled() (bool, error) {
	if p.MigrationEnabled {
		if util.IsStringEmpty(p.MigrationTopic) {
			return false, errors2.New("kafka topic for migration (%s) - but migration_topic is missing ", p.Name)
		}
		if util.IsStringEmpty(p.MigrationEndpoint) {
			return false, errors2.New("kafka topic for migration (%s) - but migration_endpoint is missing ", p.Name)
		}
		return true, nil
	}
	return false, nil
}
