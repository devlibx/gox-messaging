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

---

# Google Pub/Sub

### Setting up Pub/Sub in local system

Use following command to run it locally

```shell
docker run --rm -it -p 8085:8085 google/cloud-sdk gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
```

### Send data using Pub/Sub

Full example can be found in ```./example/main.go``` and ```./example/pubsub_send_message.go```

Here is an example to produce and consume messages from a Pub/Sub topic:
```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/devlibx/gox-base/v2"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/devlibx/gox-messaging/v2/pubsub"
	"github.com/google/uuid"
)

func PubSubSendMessage(cf gox.CrossFunction) error {

	// Setup 1 - create a producer
	producerConfig := messaging.ProducerConfig{
		Name:        "my-pubsub-topic",
		Type:        "pubsub",
		Topic:       "my-pubsub-topic",
		Concurrency: 1,
		Enabled:     true,
		Properties: gox.StringObjectMap{
			"project": "your-gcp-project-id",
		},
	}

	producer, err := pubsub.NewPubSubProducer(cf.Logger(), producerConfig)
	if err != nil {
		return err
	}

	// Send a message
	id := uuid.NewString()
	response := <-producer.Send(context.Background(), &messaging.Message{
		Key:     "key-harish-",
		Payload: map[string]interface{}{"key": "value", "id": id, "time": time.Now().String()},
	})
	if response.Err != nil {
		return response.Err
	}
	fmt.Println("Sent message with ID:", response.RawPayload)

	// Setup 2 - create a consumer
	consumerConfig := messaging.ConsumerConfig{
		Name:        "my-pubsub-topic",
		Type:        "pubsub",
		Topic:       "my-pubsub-topic",
		Concurrency: 1,
		Enabled:     true,
		Properties: gox.StringObjectMap{
			"project":      "your-gcp-project-id",
			"subscription": "my-pubsub-subscription",
		},
	}
	consumer, err := pubsub.NewPubSubConsumer(cf.Logger(), consumerConfig)
	if err != nil {
		return err
	}

	// Start consumer
	err = consumer.Process(context.Background(), messaging.NewSimpleConsumeFunction(cf, "my-consumer",
		func(message *messaging.Message) error {
			fmt.Println("Received message:", message.Payload)
			return nil
		},
		func(message *messaging.Message, err error) {
			fmt.Println("Error processing message:", err)
		},
	))
	if err != nil {
		return err
	}

	// Wait for a message to be received
	time.Sleep(10 * time.Second)

	return nil
}
```

---


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

### Redis

The Redis implementation provides a robust, distributed task queue with support for delayed messages, retries with exponential backoff, and atomic job management.

### Architecture
- **Scheduled Queue:** Holds messages with a future execution time (`MessageDelayInMs > 0`).
- **Runnable Queue:** Holds messages ready for immediate processing.
- **Visibility Queue:** Holds messages currently being processed by a worker. If a worker fails to acknowledge within the timeout, the message is automatically moved back to the Runnable queue.

### Send data using Redis

```go
func RedisExample(cf gox.CrossFunction) error {
    // 1. Setup Producer
    producerConfig := messaging.ProducerConfig{
        Name:     "my-redis-topic",
        Type:     "redis",
        Endpoint: "localhost:6379",
        Topic:    "my-topic",
        Enabled:  true,
        Properties: gox.StringObjectMap{
            "max_attempts":         5,      // Max retries for a job
            "visibility_timeout_ms": 30000,  // Initial processing timeout (30s)
        },
    }
    producer, _ := redis.NewRedisProducer(cf, producerConfig)

    // Send a delayed message (executes after 5 seconds)
    producer.Send(context.Background(), &messaging.Message{
        Key:              "job-123",
        Payload:          "hello world",
        MessageDelayInMs: 5000,
    })

    // 2. Setup Consumer
    consumerConfig := messaging.ConsumerConfig{
        Name:     "my-redis-consumer",
        Type:     "redis",
        Endpoint: "localhost:6379",
        Topic:    "my-topic",
        Enabled:  true,
        Concurrency: 10, // 10 parallel workers
        Properties: gox.StringObjectMap{
            "batch_size":                20,
            "max_visibility_timeout_ms": 300000, // Max backoff limit (5m)
        },
    }
    consumer, _ := redis.NewRedisConsumer(cf, consumerConfig)

    // Process messages
    consumer.Process(context.Background(), messaging.NewSimpleConsumeFunction(cf, "worker",
        func(message *messaging.Message) error {
            fmt.Println("Processing:", message.Key, message.Payload)
            return nil // Return nil to ACK
        },
        func(message *messaging.Message, err error) {
            fmt.Println("Failed:", message.Key, err)
        },
    ))

    return nil
}
```

### Throttling
To protect Redis memory, the producer includes built-in throttling. When the queue size exceeds a threshold, the producer will automatically slow down.

| Property | Default | Description |
| :--- | :--- | :--- |
| `throttle_scheduled_job_count` | 10,000 | Max jobs allowed in scheduled queue before throttling |
| `throttle_runnable_job_count` | 10,000 | Max jobs allowed in runnable queue before throttling |
| `throttle_delay_ms_after_scheduled_job_count_breach` | 5ms | Delay per send when scheduled limit is hit |
| `throttle_delay_ms_after_runnable_job_count_breach` | 5ms | Delay per send when runnable limit is hit |
| `password` | "" | Redis password for authentication |
| `tls_enabled` | false | Enable TLS for secure connections (e.g., AWS ElastiCache) |
| `cluster_mode` | false | Force Redis Cluster mode even with a single configuration endpoint |
| `db` | 0 | Redis database index (0-15) |
| `idempotent` | false | If true, sending a message with an existing Key will be ignored (no update to payload or queue position) |

### Idempotency
The Redis producer supports idempotent message sending. When enabled via `idempotent: true`, the producer uses the message `Key` to ensure that a message is only queued once. 

If you attempt to send a message with a `Key` that already exists in the system:
1. The existing payload in Redis will **not** be overwritten.
2. The message's position in the queue (scheduled or runnable) will **not** be updated.
3. The `Send` operation will return a successful response to maintain backward compatibility.

This is particularly useful for avoiding duplicate tasks in distributed systems where retries might occur at the producer level.

### Priority Support
The Redis implementation supports priority-based message processing. When enabled, jobs with higher priority (lower numerical value) are processed before lower priority jobs.

To enable priority support, set `priority_enabled: true` in both Producer and Consumer properties.

#### Key Features:
- **Isolation:** Enabling priority mode uses a separate set of queues (suffixed with `_prio`, e.g., `runnable_prio_jobs`) to ensure no interference with existing standard traffic.
- **Priority Values:** `0` is the highest priority. Higher numerical values represent lower priority.
- **Retries:** Priority is strictly maintained even during retries and exponential backoffs.

#### Example:
```go
// 1. Setup Producer with Priority Enabled
producerConfig := messaging.ProducerConfig{
    // ... other config ...
    Properties: gox.StringObjectMap{
        "priority_enabled": true,
    },
}
producer, _ := redis.NewRedisProducer(cf, producerConfig)

// 2. Send messages with different priorities
producer.Send(ctx, &messaging.Message{
    Key:      "urgent-job",
    Payload:  "critical data",
    Priority: 0, // Highest Priority
})

producer.Send(ctx, &messaging.Message{
    Key:      "normal-job",
    Payload:  "standard data",
    Priority: 10, // Lower Priority
})

// 3. Setup Consumer with Priority Enabled
consumerConfig := messaging.ConsumerConfig{
    // ... other config ...
    Properties: gox.StringObjectMap{
        "priority_enabled": true,
    },
}
consumer, _ := redis.NewRedisConsumer(cf, consumerConfig)
```

---

# Metric
If metrics is enabled then you can plot the following:
1. <prefix>_message_send_...     = {topic} {status=ok|error} {error=<error types>} {mode=sync|async}
   Error:
   produce_failed = failed while calling producer.Send
   failed_after_produce = send worked but got error from broker
   timeout = timeout in sending
   payload_error = something is wrong in the payload which you are sending
2. <prefix>_message_consumed_... = {topic} {status=ok|error} {error=<error types>} {mode=sync|async}
   