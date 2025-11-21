package types

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/serialization"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/devlibx/gox-messaging/v2/factory"
	"os"
	"testing"
)

//go:embed example_kafka_config.yaml
var exampleKafkaYml string

func TestKafkaEndToEndTest(t *testing.T) {
	if os.Getenv("WIKI_EXAMPLE") != "true" {
		t.Skip("enable it by setting WIKI_EXAMPLE=true")
	}

	// Read config from some file
	_msgConfig := exampleKafkaConfig{}
	err := serialization.ReadYamlFromString(exampleKafkaYml, &_msgConfig)
	if err != nil {
		panic(err)
	}

	// Setup 1 - create a message factory
	messageFactory := factory.NewMessagingFactory(gox.NewNoOpCrossFunction())
	if err := messageFactory.Start(_msgConfig.Messaging); err != nil {
		panic(err)
	}

	// Setup 2 - Get the producer to send message
	producer, err := messageFactory.GetProducer("internal_kafka_topic")
	if err != nil {
		panic(err)
	}

	// Send message and get the result
	resultCh := producer.Send(context.Background(), &messaging.Message{
		Key:     "1235",
		Payload: map[string]interface{}{"int": 10, "string": "Str"},
	})
	result := <-resultCh
	if result.Err != nil {
		panic(result.Err)
	}
	fmt.Println(result.RawPayload)
}

// exampleKafkaConfig is a container to read yaml. You can put the messaging.Configuration in your own
// config struct
type exampleKafkaConfig struct {
	Messaging messaging.Configuration `yaml:"messaging"`
}
