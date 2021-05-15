package main

import (
	"fmt"
	"github.com/devlibx/gox-base"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/devlibx/gox-messaging/kafka"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
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
		Name:        "test_consumer",
		Type:        "kafka",
		Endpoint:    "localhost:9092",
		Topic:       "test",
		Concurrency: 1,
		Enabled:     true,
		Properties:  map[string]interface{}{"group.id": "some", "auto.offset.reset": "earliest"},
	}

	configs := messaging.Configuration{
		Enabled:   true,
		Producers: map[string]messaging.ProducerConfig{"test": pc},
		Consumers: map[string]messaging.ConsumerConfig{"test_consumer": cc},
	}

	f := kafka.NewKafkaMessagingFactory(gox.NewNoOpCrossFunction())
	err := f.Start(configs)
	if err != nil {
		panic("Error")
	}

	if c, err := f.GetConsumer("test_consumer"); err != nil {
		panic("Error to open consumer")
	} else {
		_ = c
		/*_ = c.Start(func(message *messaging.Message) error {
			fmt.Printf("key=%s, value=%s \n", message.Key, string(message.Data))
			return nil
		})*/
	}

	count := int64(0)
	if p, err := f.GetProducer("test"); err != nil {
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
				for i := 0; i < 100 && end; i++ {
					atomic.AddInt64(&count, 1)
					/*errCh := p.Send(context.TODO(), "1_"+strconv.Itoa(int(count)), []byte("data_"+strconv.Itoa(threadId)+"_"+strconv.Itoa(int(count))))
					select {
					case err := <-errCh:
						if err != nil {
							// fmt.Println("Got error: ", err, "count=", count)
						}
					case <-time.After(10 * time.Second):
						end = false
						panic("We should not get this")
					}*/
				}
				wg.Done()
			}(j)
		}
		wg.Wait()
	}
	fmt.Println("Total Messages:", count)
	// time.Sleep(time.Hour)
}
