This library provided unified way to send messages to SQS and Kafka. It also provides a dummy queue implementation for helping in test

---

# SQS

### Setting up SQS in local system

Use following command to run it locally

```shell
docker run --rm -it -p 8000:4566 -p 4571:4571 localstack/localstack
```

###### Useful commands

```shell
Create a new queue:
===================
aws --endpoint=http://localhost:4566 sqs create-queue --queue-name test_queue_in_sqs

Read messages from queue:
========================
aws --endpoint=http://localhost:8000 sqs receive-message --queue-url  http://localhost:8000/000000000000/test_queue_in_sqs

Send messages from queue:
========================
aws --endpoint=http://localhost:8000 sqs send-message --queue-url http://localhost:8000/000000000000/test_queue_in_sqs --message-body "{'body': 'abcd'}"
```

### Send data using SQS

Full example ```./example/main.go and ./example/sqs_send_message.go```

```
func SqsSendMessage(cf gox.CrossFunction) error {
	awsctx, err := goxAws.NewAwsContext(cf, goxAws.Config{
		Endpoint: "http://localhost:8000",
		Region:   "us-east-1",
	})
	if err != nil {
		return err
	}

	// Setup 1 - create a producer
	producerConfig := messaging.ProducerConfig{
		Name:        "test_queue",
		Type:        "sqs",
		Topic:       "http://localhost:8000/000000000000/test_queue_in_sqs",
		Concurrency: 1,
		Enabled:     true,
		AwsContext:  awsctx,
	}

	producer, err := sqs.NewSqsProducer(cf, producerConfig)
	if err != nil {
		return err
	}

	contextWithTimeout, contextCancelFunction := context.WithTimeout(context.Background(), 1*time.Second)
	defer contextCancelFunction()

	// Send a message 
	id := uuid.NewString()
	response := <-producer.Send(contextWithTimeout, &messaging.Message{
		Key:     "key",
		Payload: map[string]interface{}{"key": "value", "id": id},
	})
	if response.Err != nil {
		return response.Err
	}
	fmt.Println(response.RawPayload)
	return nil
}

```

---

# Kafka
NOTE - properties for kafka are define in https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

### Send data using Kafka

Producer and Consumer example can be found int ```kafka/producer_test.go``` and ```kafka/consumer_test.go```

Here is a example to consume messages from some topic from kafka:
```go
consumer, err := NewKafkaConsumer(gox.NewNoOpCrossFunction(), consumerConfig)
if err != nil {
    return errors.Wrap(err, "failed to get consumer")
}

err = consumer.Process(ctx, messaging.NewSimpleConsumeFunction
    (
        gox.NewNoOpCrossFunction(),
        "any-name",
        func(message *messaging.Message) error {
			// Process message here
            return nil
        },
        func(message *messaging.Message, err error) {
            // Process error here - if there is some error in getting message
			// from kafka, this callback method is called
        },
    ),
)
if err != nil {
    return errors.Wrap(err, "some error in setting up a consumer to consume messages")
}
```

##### Getting errors from async kafka publish
You can get the error raised when oyu use async kafka mode. We have a common way to deliver errors using error channel.
This works for async/sync mode (both)

Since not all messaging, support error reporting in all modes; we have added support for ErrorReporter. If a producer is 
a ErrorReporter, then you can use it to get error reporting from error channel 
```go

// Set property "error_reporting_channel_size" to > 0 to enable it. Do not keep this value very small. 
// error_reporting_channel_size > 10000 or 1000000 is Ok
// NOTE: If you do not consume from GetErrorReport() channel, the producer amy block eventually (we have added
// some level of protection on this channel to timeout error reporting after 10ms - but it is still not the best way) 

if r, ok := producer.(messaging.ErrorReporter); ok {
     go func() {
         if ch, enabled, err := r.GetErrorReport(); err == nil && enabled {
             for errorR := range ch {
                 fmt.Println(errorR.Err, errorR.RawPayload)
             }
         }
     }()
 }
```

### Metric
If metrics is enabled then you can plot the following:
1. <prefix>_message_send_...     = {topic} {status=ok|error} {error=<error types>} {mode=sync|async}
   Error:
   produce_failed = failed while calling producer.Send
   failed_after_produce = send worked but got error from broker
   timeout = timeout in sending
   payload_error = something is wrong in the payload which you are sending
2. <prefix>_message_consumed_... = {topic} {status=ok|error} {error=<error types>} {mode=sync|async}

---

# Working example Kafka & SQS
From user point of view, there is not difference between SQS and Kafka producer or consumer code. The only change is in config file where 
you can change the type to `sqs` or `kafka`

### Kafka
`example_kafka_config.yaml`
```yaml
messaging:
  enabled: true
  producers:
    internal_kafka_topic:
      type: kafka
      endpoint: "localhost:9092"
      topic: test
      concurrency: 1
      enabled: true
      properties:
        publish.message.timeout.ms: 100
        acks: 1
```
```go
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
)

//go:embed example_kafka_config.yaml
var exampleKafkaYml string

func TestKafkaEndToEndTest(t *testing.T) {
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
```

###  SQS
`example_sqs_config.yaml`
```yaml
messaging:
  enabled: true
  producers:
    internal_sql_topic:
      type: sqs
      topic: <SQS QUEUE URL>
      concurrency: 2
      enabled: true
  consumers:
    internal_sql_topic:
      type: sqs
      topic: <SQS QUEUE URL>
      concurrency: 2
      enabled: true
```
```go
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

```
