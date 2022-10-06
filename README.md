This library provided unified way to send messages to SQS and Kafka. It also provides a dummy queue implementation for helping in test

---

# SQS

### Setting up SQS in local system

Use following commaand to run it locally

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