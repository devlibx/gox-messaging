package messaging

import (
	goxAws "github.com/devlibx/gox-aws"
	"github.com/google/uuid"
)

type DummyProducerFunc func(key string, value []byte) error

type ProducerConfig struct {
	Name                                   string
	Type                                   string                 `yaml:"type"`
	Endpoint                               string                 `yaml:"endpoint"`
	Topic                                  string                 `yaml:"topic"`
	Concurrency                            int                    `yaml:"concurrency"`
	Enabled                                bool                   `yaml:"enabled"`
	Properties                             map[string]interface{} `yaml:"properties"`
	Async                                  bool                   `yaml:"async"`
	MessageTimeoutInMs                     int                    `yaml:"message_timeout_ms"`
	EnableArtificialDelayToSimulateLatency bool                   `yaml:"enable_artificial_delay_to_simulate_latency"`
	MaxMessageInBuffer                     int                    `yaml:"max_message_in_buffer"`
	DummyProducerFunc                      DummyProducerFunc
	AwsContext                             goxAws.AwsContext
}

type ConsumerConfig struct {
	Name        string
	Type        string                 `yaml:"type"`
	Endpoint    string                 `yaml:"endpoint"`
	Topic       string                 `yaml:"topic"`
	Concurrency int                    `yaml:"concurrency"`
	Enabled     bool                   `yaml:"enabled"`
	Properties  map[string]interface{} `yaml:"properties"`
	AwsContext                             goxAws.AwsContext
}

type Configuration struct {
	Enabled   bool                      `yaml:"enabled"`
	Producers map[string]ProducerConfig `yaml:"producers"`
	Consumers map[string]ConsumerConfig `yaml:"consumers"`
}

func (p *ProducerConfig) SetupDefaults() {
	if p.Properties == nil {
		p.Properties = map[string]interface{}{}
	}
	if _, ok := p.Properties["acks"].(string); !ok {
		p.Properties["acks"] = "all"
	}
	if p.MessageTimeoutInMs <= 0 {
		p.MessageTimeoutInMs = 20
	}
	if p.MaxMessageInBuffer <= 0 {
		p.MaxMessageInBuffer = 1000
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
		p.Properties["auto.offset.reset"] = "earliest"
	}
}