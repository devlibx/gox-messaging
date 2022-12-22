package main

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/metrics"
	"github.com/devlibx/gox-base/util"
	messaging "github.com/devlibx/gox-messaging"
	"github.com/devlibx/gox-messaging/kafka"
	"github.com/devlibx/gox-metrics/provider/multi"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"os"
	"time"
)

func main() {

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
	metricObj, err := multi.NewRootScope(MetricConfig)
	if err != nil {
		panic(err)
	}

	zapConfig := zap.NewDevelopmentConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	z, _ := zapConfig.Build()
	cf := gox.NewCrossFunction(z, metricObj)

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
			messaging.KMessagingPropertyPublishMessageTimeoutMs: 100,
			messaging.KMessagingPropertyAcks:                    "1",
		},
	}

	producer, err := kafka.NewKafkaProducer(cf, producerConfig)
	if err != nil {
		return err
	}

	contextWithTimeout, contextCancelFunction := context.WithTimeout(context.Background(), 100*time.Second)
	defer contextCancelFunction()

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
			fmt.Println(response.RawPayload)
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(time.Second)
	return nil
}
