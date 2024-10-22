package main

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
	"time"
)

//go:embed example_sqs_config.yaml
var exampleSqlYml string

func TestSeqEndToEndTest(t *testing.T) {
	if os.Getenv("WIKI_EXAMPLE") != "true" && false {
		t.Skip("enable it by setting WIKI_EXAMPLE=true")
	}

	// Read config from some file
	_msgConfig := exampleSqsConfig{}
	err := serialization.ReadYamlFromString(exampleSqlYml, &_msgConfig)
	if err != nil {
		panic(err)
	}

	// Setup 1 - create a message factory
	messageFactory := factory.NewMessagingFactory(gox.NewNoOpCrossFunction())
	if err := messageFactory.Start(_msgConfig.Messaging); err != nil {
		panic(err)
	}

	// Setup 2 - Get the producer to send message
	producer, err := messageFactory.GetProducer("internal_sql_topic")
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
	time.Sleep(time.Second)

	// Setup 2 - Get the producer to send message
	consumer, err := messageFactory.GetConsumer("internal_sql_topic")
	if err != nil {
		panic(err)
	}

	// Add a consumer function to consume messages
	_ = consumer.Process(context.Background(),
		messaging.NewSimpleConsumeFunction(gox.NewNoOpCrossFunction(),
			"some_name_for_logging",
			func(message *messaging.Message) error {
				payload, _ := message.PayloadAsString()
				fmt.Println("Got message in consumer", payload)
				return nil
			}, func(message *messaging.Message, err error) {
				fmt.Println("Got error in consumer", err)
			},
		))

	time.Sleep(2 * time.Second)

}

// exampleKafkaConfig is a container to read yaml. You can put the messaging.Configuration in your own
// config struct
type exampleSqsConfig struct {
	Messaging messaging.Configuration `yaml:"messaging"`
}
