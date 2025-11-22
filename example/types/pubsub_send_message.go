package types

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/devlibx/gox-base/v2"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/devlibx/gox-messaging/v2/pubsub"
	"github.com/google/uuid"
)

func PubSubSendMessage(cf gox.CrossFunction) error {

	projectId := strings.TrimSpace(os.Getenv("GOOGLE_PROJECT_ID"))
	topicName := strings.TrimSpace(os.Getenv("GOOGLE_PUB_TOPIC"))
	subscriptionName := strings.TrimSpace(os.Getenv("GOOGLE_PUB_SUB"))

	if projectId == "" || topicName == "" || subscriptionName == "" {
		return fmt.Errorf("missing environment variables: GOOGLE_PROJECT_ID, GOOGLE_PUB_TOPIC, GOOGLE_PUB_SUB")
	}

	// Setup 1 - create a producer
	producerConfig := messaging.ProducerConfig{
		Name:        "my-pubsub-topic",
		Type:        "pubsub",
		Topic:       topicName,
		Concurrency: 1,
		Enabled:     true,
		Properties: gox.StringObjectMap{
			"project":          projectId,
			"json_credentials": os.Getenv("GOOGLE_CREDENTIALS"),
		},
	}

	producer, err := pubsub.NewPubSubProducer(cf.Logger(), producerConfig)
	if err != nil {
		return err
	}

	threads := 10
	const maxMessages int64 = 10000

	start := time.Now()
	var messages int64 = 0
	wg := &sync.WaitGroup{}
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			for {
				count := atomic.AddInt64(&messages, 1)
				if count >= maxMessages {
					wg.Done()
					break
				}
				// Send a message
				id := uuid.NewString()
				response := <-producer.Send(context.Background(), &messaging.Message{
					Key:     "key-harish-",
					Payload: map[string]interface{}{"key": "value", "id": id, "time": time.Now().String()},
				})
				if response.Err != nil {
					fmt.Printf("%d - Sent message with ID = %s\n", count, err.Error())
				} else {
					if count%1000 == 0 {
						fmt.Printf("%d - Sent message with ID = %s\n", count, response.RawPayload)
					}
				}
			}
		}()
	}
	wg.Wait()
	end := time.Now()
	fmt.Printf("Published %d messages in %v\n", messages, end.Sub(start))

	// Setup 2 - create a consumer
	consumerConfig := messaging.ConsumerConfig{
		Name:        "my-pubsub-topic",
		Type:        "pubsub",
		Topic:       subscriptionName,
		Concurrency: 1,
		Enabled:     true,
		Properties: gox.StringObjectMap{
			"project":          projectId,
			"json_credentials": os.Getenv("GOOGLE_CREDENTIALS"),
		},
	}
	consumer, err := pubsub.NewPubSubConsumer(cf.Logger(), consumerConfig)
	if err != nil {
		return err
	}

	// Start consumer
	start = time.Now()
	var consumedMessages int64 = 0
	err = consumer.Process(context.Background(), messaging.NewSimpleConsumeFunction(cf, "my-consumer",
		func(message *messaging.Message) error {
			count := atomic.AddInt64(&consumedMessages, 1)
			if count%1000 == 0 {
				fmt.Println("Received message:", message.Payload)
				fmt.Println(gox.StringObjectMapFromJson(string(message.Payload.([]byte))))
			}
			return nil
		},
		func(message *messaging.Message, err error) {
			count := atomic.AddInt64(&consumedMessages, 1)
			if count%1000 == 0 {
				fmt.Println("Error processing message:", err)
				fmt.Println(gox.StringObjectMapFromJson(string(message.Payload.([]byte))))
			}
		},
	))
	if err != nil {
		return err
	}
	for {
		count := atomic.AddInt64(&consumedMessages, 0)
		if count >= maxMessages-10 {
			break
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	end = time.Now()
	fmt.Printf("Consumed %d messages in %v\n", consumedMessages, end.Sub(start))

	// Wait for a message to be received
	time.Sleep(60 * time.Second)

	return nil
}
