package kafka

import (
	"context"
	"flag"
	"fmt"
	goxAws "github.com/devlibx/gox-aws/v2"
	"github.com/devlibx/gox-base/v2/test"
	"github.com/devlibx/gox-base/v2/util"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"os"
	"sync"
	"testing"
	"time"
)

var queue string

func init() {
	var ignore string
	flag.StringVar(&queue, "real.kafka.topic", "", "Sqs queue to ues for testing")
	flag.StringVar(&ignore, "real.sqs.queue", "", "Sqs queue to ues for testing")
}

func TestSqsSendV1(t *testing.T) {
	if util.IsStringEmpty(queue) {
		t.Skip("Need to pass Kafka Queue using -real.kafka.topic=<name>")
	}

	cf, _ := test.MockCf(t, zap.DebugLevel)
	ctx, err := goxAws.NewAwsContext(cf, goxAws.Config{})
	assert.NoError(t, err)

	producerConfig := messaging.ProducerConfig{
		Name:                                   "test",
		Type:                                   "kafka",
		Endpoint:                               "localhost:9092",
		Topic:                                  queue,
		Concurrency:                            100,
		Enabled:                                true,
		Properties:                             gox.StringObjectMap{},
		Async:                                  false,
		AwsContext:                             ctx,
		EnableArtificialDelayToSimulateLatency: true,
	}

	producer, err := NewKafkaProducer(cf, producerConfig)
	assert.NoError(t, err)

	// Test 1 - Test sync message send
	c, _ := context.WithTimeout(context.Background(), 1*time.Second)
	response := <-producer.Send(c, &messaging.Message{
		Key:     "key",
		Payload: map[string]interface{}{"key": "value"},
	})
	assert.NoError(t, response.Err)
	assert.NotNil(t, response.RawPayload)
	cf.Logger().Debug("Output from Kafka", zap.Any("sqsResponse", response.RawPayload))

	/*// Test 2 - Test sync message send failed due to context timeout
	c, _ = context.WithTimeout(context.Background(), 10*time.Millisecond)
	response = <-producer.Send(c, &messaging.Message{
		Key:                              "key",
		Payload:                          map[string]interface{}{"key": "value"},
		ArtificialDelayToSimulateLatency: 100 * time.Millisecond,
	})
	assert.Error(t, response.Err)
	cf.Logger().Debug("Output from SQS", zap.Any("sqsResponse", response.Err))*/

	// Test 3 - do not send after producer is closed
	_ = producer.Stop()
	response = <-producer.Send(context.TODO(), &messaging.Message{
		Key:     "key",
		Payload: map[string]interface{}{"key": "value"},
	})
	assert.Error(t, response.Err)
	cf.Logger().Debug("Output from Kafka", zap.Any("sqsResponse", response))
}

func TestKafkaSendV1(t *testing.T) {
	kafkaTopicName := os.Getenv("KAFKA_TOPIC")
	if util.IsStringEmpty(kafkaTopicName) {
		t.Skip("Need to pass kafka topic name using env var KAFKA_TOPIC")
	}

	cf, _ := test.MockCf(t, zap.InfoLevel)
	ctx, err := goxAws.NewAwsContext(cf, goxAws.Config{})
	assert.NoError(t, err)

	// Consumer setup start
	consumedMessageWg := sync.WaitGroup{}
	consumerConfig := messaging.ConsumerConfig{
		Name:        "test",
		Type:        "kafka",
		Topic:       kafkaTopicName,
		Endpoint:    "localhost:9092",
		Concurrency: 10,
		Enabled:     true,
		Properties: map[string]interface{}{
			"group.id": uuid.NewString(),
			messaging.KMessagingPropertyRateLimitPerSec:                10000,
			messaging.KMessagingPropertyPartitionProcessingParallelism: 10,
		},
		AwsContext: ctx,
	}
	cf.Logger().Info("Start kafka consumer")
	consumer, err := NewKafkaConsumer(cf, consumerConfig)
	assert.NoError(t, err)
	counter := 0
	err = consumer.Process(context.Background(), messaging.NewSimpleConsumeFunction(cf, kafkaTopicName, func(message *messaging.Message) error {
		counter++
		if counter%1000 == 0 {
			s, _ := message.PayloadAsString()
			fmt.Println("-->>>", counter, "   ", s)
		}
		consumedMessageWg.Done()
		return nil
	}, func(message *messaging.Message, err error) {
		counter++
		fmt.Println("-->>> error ", counter)
		consumedMessageWg.Done()
	}))
	if err != nil {
		assert.NoError(t, err)
	}
	time.Sleep(5 * time.Second)
	fmt.Println("Consumer started and got the consumer group...")
	// Consumer setup end

	producerConfig := messaging.ProducerConfig{
		Name:        "test",
		Type:        "kafka",
		Endpoint:    "localhost:9092",
		Topic:       kafkaTopicName,
		Concurrency: 10,
		Enabled:     true,
		Properties: gox.StringObjectMap{
			messaging.KMessagingPropertyPublishMessageTimeoutMs: 1000,
			// messaging.KMessagingPropertyLingerMs:                      10,
			// messaging.KMessagingPropertyBatchSize:                     65000,
			// messaging.KMessagingPropertyCompressionType:               "gzip",
		},
		Async:                                  false,
		AwsContext:                             ctx,
		EnableArtificialDelayToSimulateLatency: true,
	}

	producer, err := NewKafkaProducer(cf, producerConfig)
	assert.NoError(t, err)

	threads := 20
	messageCount := 10000

	start := time.Now()
	sg := sync.WaitGroup{}
	c, _ := context.WithTimeout(context.Background(), 20*time.Second)
	ch := make(chan messaging.Message, threads)
	for i := 0; i < threads; i++ {
		go func(idx int) {
			for m := range ch {
				response := <-producer.Send(c, &m)
				sg.Done()
				assert.NoError(t, response.Err)
				assert.NotNil(t, response.RawPayload)
				cf.Logger().Debug("Output from Kafka", zap.Any("sqsResponse", response.RawPayload))
			}
		}(i)
	}

	// Test 1 - Test sync message send
	for i := 0; i < messageCount; i++ {
		sg.Add(1)
		consumedMessageWg.Add(1)
		ch <- messaging.Message{
			Key:     "key",
			Payload: map[string]interface{}{"key": "value", "index": i, "run": "2"},
		}
	}

	sg.Wait()
	end := time.Now()
	fmt.Println("Time to send messages = ", (end.UnixMilli()-start.UnixMilli())/1000, " sec")

	consumedMessageWg.Wait()
	consumerEnd := time.Now()
	fmt.Println("Time to read messages = ", (consumerEnd.UnixMilli()-start.UnixMilli())/1000, " sec")
}
