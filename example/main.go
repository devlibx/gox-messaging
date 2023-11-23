package main

import (
	"context"
	"fmt"
	_ "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/metrics"
	"github.com/devlibx/gox-base/util"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/devlibx/gox-messaging/kafka"
	"github.com/devlibx/gox-metrics/provider/multi"
	"github.com/google/uuid"
	"go.uber.org/zap"
	context2 "golang.org/x/net/context"
	"os"
	"sync/atomic"
	"time"
)

func main() {

	var metricObj metrics.ClosableScope
	var err error
	if false {
		statsDHost := os.Getenv("__STATSD__")
		var MetricConfig metrics.Config
		MetricConfig.Enabled = true
		MetricConfig.Prefix = "test_metric_messaging"
		MetricConfig.ReportingIntervalMs = 1000
		if util.IsStringEmpty(statsDHost) {
			MetricConfig.EnableStatsd = false
		} else {
			MetricConfig.EnableStatsd = true
			MetricConfig.Statsd.Address = statsDHost
			MetricConfig.Statsd.FlushIntervalMs = 10
			MetricConfig.Statsd.FlushBytes = 1440
			MetricConfig.Statsd.Properties = map[string]interface{}{"comma_perpetrated_stats_reporter": true}
		}
		metricObj, err = multi.NewRootScope(MetricConfig)
		if err != nil {
			panic(err)
		}
	}

	zapConfig := zap.NewDevelopmentConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	z, _ := zapConfig.Build()
	var cf gox.CrossFunction
	if metricObj == nil {
		cf = gox.NewCrossFunction(z)
	} else {
		cf = gox.NewCrossFunction(z, metricObj)
	}

	// Send SQS message
	// err := SqsSendMessage(cf)
	err = KafkaSendMessage(cf)
	if err != nil {
		panic(err)
	}
	time.Sleep(2 * time.Second)
}

func KafkaSendMessage(cf gox.CrossFunction) error {

	// Setup 1 - create a producer
	producerConfig := messaging.ProducerConfig{
		Name:        "test_queue",
		Type:        "kafka",
		Endpoint:    "localhost:9092",
		Topic:       "harish_test",
		Concurrency: 1,
		Enabled:     true,
		Async:       false,
		Properties: gox.StringObjectMap{
			messaging.KMessagingPropertyPublishMessageTimeoutMs:   100,
			messaging.KMessagingPropertyAcks:                      "1",
			messaging.KMessagingPropertyErrorReportingChannelSize: 100,
		},
		KafkaSpecificProperty: map[string]interface{}{
			"acks":             "0",
			"compression.type": "gzip",
		},
	}

	producer, err := kafka.NewKafkaProducer(cf, producerConfig)
	if err != nil {
		return err
	}

	if r, ok := producer.(messaging.ErrorReporter); ok {
		fmt.Println("")
		go func() {
			if ch, enabled, err := r.GetErrorReport(); err == nil && enabled {
				for errorR := range ch {
					fmt.Println(errorR.Err, errorR.RawPayload)
				}
			}
		}()
	}

	contextWithTimeout, contextCancelFunction := context.WithTimeout(context.Background(), 100*time.Second)
	defer contextCancelFunction()

	go func() {
		KafkaReadMessage(cf, "harish_test")
	}()

	// Send a message
	for {
		id := uuid.NewString()
		response := <-producer.Send(contextWithTimeout, &messaging.Message{
			Key:     "key",
			Payload: map[string]interface{}{"key": "value", "id": id},
		})
		if response.Err != nil {
			fmt.Println("Error =>>>>", response.Err)
		} else {
			//	fmt.Println(response.RawPayload)
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(time.Second)
	return nil
}

func KafkaReadMessage(cf gox.CrossFunction, kafkaTopicName string) {
	consumerConfig := messaging.ConsumerConfig{
		Name:        "test",
		Type:        "kafka",
		Topic:       kafkaTopicName,
		Endpoint:    "localhost:9092",
		Concurrency: 2,
		Enabled:     true,
		Properties: map[string]interface{}{
			"group.id": uuid.NewString(),
			messaging.KMessagingPropertyRateLimitPerSec:         10,
			messaging.KMessagingPropertyPublishMessageTimeoutMs: 10000,
		},
		KafkaSpecificProperty: map[string]interface{}{
			"acks":             "0",
			"compression.type": "gzip",
		},
	}
	// Test 1 - Read message
	cf.Logger().Info("Start kafka consumer")
	consumer, err := kafka.NewKafkaConsumer(cf, consumerConfig)
	if err != nil {
		panic(err)
	}

	var count int32 = 0
	ctx, ctxCancel := context2.WithCancel(context.TODO())
	defer ctxCancel()
	err = consumer.Process(ctx, messaging.NewSimpleConsumeFunction(
		cf,
		"consumer_test_func",
		func(message *messaging.Message) error {
			atomic.AddInt32(&count, 1)
			m, err := message.PayloadAsStringObjectMap()
			fmt.Println("Got message... ", m, err, "message_count=", count)
			return nil
		},
		nil,
	))
	time.Sleep(1 * time.Hour)
}
