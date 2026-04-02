package main

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/serialization"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/devlibx/gox-messaging/v2/factory"
	"go.uber.org/zap"
)

func main() {
	// Setup Logger
	zapConfig := zap.NewDevelopmentConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	z, _ := zapConfig.Build()
	cf := gox.NewCrossFunction(z)

	// 1. Load Configuration from YAML
	type localConfig struct {
		MessagingConfig messaging.Configuration `yaml:"messaging"`
	}
	var lConfig = localConfig{}
	
	// Get path to config.yaml in the same directory as this file
	_, filename, _, _ := runtime.Caller(0)
	configPath := filepath.Join(filepath.Dir(filename), "config.yaml")
	
	err := serialization.ReadYaml(configPath, &lConfig)
	if err != nil {
		cf.Logger().Fatal("failed to read config.yaml", zap.String("path", configPath), zap.Error(err))
	}

	// 2. Initialize Messaging Factory
	f := factory.NewMessagingFactory(cf)
	err = f.Start(lConfig.MessagingConfig)
	if err != nil {
		cf.Logger().Fatal("failed to start messaging factory", zap.Error(err))
	}
	defer f.Stop()

	// 3. Get Producer and Send Messages with Different Priorities
	// We send them in "wrong" order: P2, then P0, then P1
	p, err := f.GetProducer("priority_producer")
	if err != nil {
		cf.Logger().Fatal("failed to get producer", zap.Error(err))
	}

	fmt.Println(">>> Producing messages with mixed priorities...")
	
	// Send Priority 2 (Lowest)
	<-p.Send(context.Background(), &messaging.Message{
		Key:      "job-p2",
		Payload:  "I am Priority 2",
		Priority: 2,
	})
	fmt.Println("Sent P2")

	// Send Priority 0 (Highest)
	<-p.Send(context.Background(), &messaging.Message{
		Key:      "job-p0",
		Payload:  "I am Priority 0",
		Priority: 0,
	})
	fmt.Println("Sent P0")

	// Send Priority 1 (Medium)
	<-p.Send(context.Background(), &messaging.Message{
		Key:      "job-p1",
		Payload:  "I am Priority 1",
		Priority: 1,
	})
	fmt.Println("Sent P1")

	// 4. Setup Consumer to Process Messages
	c, err := f.GetConsumer("priority_consumer")
	if err != nil {
		cf.Logger().Fatal("failed to get consumer", zap.Error(err))
	}

	fmt.Println("\n>>> Starting consumer (should process in P0 -> P1 -> P2 order)...")
	
	// Process function
	consumeFunc := messaging.NewSimpleConsumeFunction(cf, "prio-worker",
		func(message *messaging.Message) error {
			fmt.Printf(" [WORKER] Processed: %s (Priority: %d, Payload: %v)\n", 
				message.Key, message.Priority, message.Payload)
			return nil
		},
		func(message *messaging.Message, err error) {
			cf.Logger().Error("error in processing", zap.Any("message", message), zap.Error(err))
		},
	)

	// Start Processing
	err = c.Process(context.Background(), consumeFunc)
	if err != nil {
		cf.Logger().Fatal("failed to start consumer processing", zap.Error(err))
	}

	// Wait for all messages to be processed
	time.Sleep(5 * time.Second)
	fmt.Println("\n>>> Example finished.")
}
