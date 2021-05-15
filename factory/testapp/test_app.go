package main

import (
	"context"
	"flag"
	"fmt"
	goxAws "github.com/devlibx/gox-aws"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/serialization"
	"github.com/devlibx/gox-base/util"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/devlibx/gox-messaging/factory"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var queue string

func main() {
	flag.StringVar(&queue, "real.sqs.queue", "", "Sqs queue to ues for testing")
	flag.Parse()

	// Read from env variable
	if util.IsStringEmpty(queue) {
		queue = os.Getenv("TEST_SQS_QUEUE")
	}

	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)

	cf := gox.NewCrossFunction(config.Build())
	ctx, err := goxAws.NewAwsContext(cf, goxAws.Config{})

	pc := messaging.ProducerConfig{
		Name:               "test",
		Type:               "kafka",
		Endpoint:           "localhost:9092",
		Topic:              "test",
		Concurrency:        10,
		Enabled:            true,
		Properties:         map[string]interface{}{"acks": "0"},
		Async:              false,
		MessageTimeoutInMs: 100,
		DummyProducerFunc:  nil,
	}

	cc := messaging.ConsumerConfig{
		Name:        "test",
		Type:        "kafka",
		Endpoint:    "localhost:9092",
		Topic:       "test",
		Concurrency: 1,
		Enabled:     true,
		Properties:  map[string]interface{}{"group.id": uuid.NewString(), "auto.offset.reset": "earliest"},
	}

	pcSqs := messaging.ProducerConfig{
		Name:               "test_sqs",
		Type:               "sqs",
		Topic:              queue,
		Concurrency:        10,
		Enabled:            true,
		Properties:         map[string]interface{}{"acks": "0"},
		Async:              false,
		MessageTimeoutInMs: 100,
		DummyProducerFunc:  nil,
		AwsContext:         ctx,
	}

	ccSqs := messaging.ConsumerConfig{
		Name:        "test_sqs",
		Type:        "sqs",
		Topic:       queue,
		Concurrency: 1,
		Enabled:     true,
		AwsContext:  ctx,
		Properties:  map[string]interface{}{"group.id": "some", "auto.offset.reset": "earliest"},
	}

	configs := messaging.Configuration{
		Enabled:   true,
		Producers: map[string]messaging.ProducerConfig{"test": pc, "test_sqs": pcSqs},
		Consumers: map[string]messaging.ConsumerConfig{"test": cc, "test_sqs": ccSqs},
	}

	f := factory.NewKafkaMessagingFactory(cf)
	err = f.Start(configs)
	if err != nil {
		panic("Error")
	}
	defer func() { _ = f.Stop() }()

	topicName := "test"
	messageCount := 5
	id := uuid.NewString()
	// Consumer function
	consumerFunc := &sqsTestConsumerFunction{
		messages:      make([]*messaging.Message, 0),
		id:            id,
		wg:            sync.WaitGroup{},
		CrossFunction: cf,
	}
	consumerFunc.wg.Add(messageCount)

	if consumer, err := f.GetConsumer(topicName); err != nil {
		panic("Error to open consumer")
	} else {
		// Start reading from consumer
		err = consumer.Process(context.TODO(), consumerFunc)
		if err != nil {
			panic("Error")
		}
		time.Sleep(1000 * time.Millisecond)
	}

	count := int64(0)
	if p, err := f.GetProducer(topicName); err != nil {
		panic("Error")
	} else {

		wg := sync.WaitGroup{}
		go func() {
			time.Sleep(time.Second)
			_ = p.Stop()
		}()

		for j := 0; j < 10; j++ {
			wg.Add(1)
			go func(threadId int) {
				end := true
				for i := 0; i < 1 && end; i++ {
					atomic.AddInt64(&count, 1)
					toSend := &messaging.Message{
						Key:     "key",
						Payload: map[string]interface{}{"key": "value_" + id, "id": id},
					}
					result := <-p.Send(context.TODO(), toSend)
					if result.Err != nil {
						fmt.Println("Got error in sending", result.Err)
					}
				}
				wg.Done()
			}(j)
		}
		wg.Wait()
	}
	fmt.Println("Total Messages:", count)

	// If test does not finish then complete it 5 sec
	time.Sleep(10 * time.Second)
	if messageCount > len(consumerFunc.messages) {
		cf.Logger().Info("Got messages", zap.Int("count", len(consumerFunc.messages)))
		panic("error")
	}

}

type sqsTestConsumerFunction struct {
	messages []*messaging.Message
	id       string
	wg       sync.WaitGroup
	gox.CrossFunction
}

func (s *sqsTestConsumerFunction) Process(message *messaging.Message) error {
	if str, ok := message.Payload.(string); ok {
		fmt.Println(string(str))
		m := gox.StringObjectMap{}
		err := serialization.JsonBytesToObject([]byte(str), &m)
		if err == nil && m["id"] == s.id {
			s.messages = append(s.messages, message)
			//s.wg.Done()
		}
	} else if str, ok := message.Payload.([]byte); ok {
		fmt.Println(string(str))
		m := gox.StringObjectMap{}
		err := serialization.JsonBytesToObject([]byte(str), &m)
		if err == nil && m["id"] == s.id {
			s.messages = append(s.messages, message)
			// s.wg.Done()
		}
	}
	return nil
}

func (s *sqsTestConsumerFunction) ErrorInProcessing(message *messaging.Message, err error) {
	// panic("implement me")
}
