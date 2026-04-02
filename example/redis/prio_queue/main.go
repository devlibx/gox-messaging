package main

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/serialization"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/devlibx/gox-messaging/v2/factory"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func main() {
	// Setup Logger
	zapConfig := zap.NewDevelopmentConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(zap.ErrorLevel) // Keep it quiet to see our prints
	z, _ := zapConfig.Build()
	cf := gox.NewCrossFunction(z)

	// 1. Load Configuration from YAML
	type localConfig struct {
		MessagingConfig messaging.Configuration `yaml:"messaging"`
	}
	var lConfig = localConfig{}
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

	// 3. Stats tracking
	var p0Sent, p1Sent, p2Sent int64
	var p0Done, p1Done, p2Done int64

	// 4. Start Consumer with higher concurrency to handle some load but still show queuing
	c, _ := f.GetConsumer("priority_consumer")
	
	// Adjust concurrency in config via code for this example if needed, 
	// but we'll assume the YAML or default is sufficient. 
	// Let's simulate a slow worker (100ms) so the queue builds up.
	consumeFunc := messaging.NewSimpleConsumeFunction(cf, "prio-worker",
		func(message *messaging.Message) error {
			switch message.Priority {
			case 0:
				atomic.AddInt64(&p0Done, 1)
			case 1:
				atomic.AddInt64(&p1Done, 1)
			case 2:
				atomic.AddInt64(&p2Done, 1)
			}
			// Removed time.Sleep to show high-performance
			return nil
		},
		func(message *messaging.Message, err error) {},
	)
	_ = c.Process(context.Background(), consumeFunc)

	// 5. Start Producer Loop (2 Minutes) - Target 5000 RPS
	p, _ := f.GetProducer("priority_producer")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	fmt.Printf(">>> Starting Extreme-Load Priority Queue Simulation (~5000 RPS)...\n")
	fmt.Printf(">>> Target: 300,000 msg/min | Total: 6,000,000 msg (2 min)\n\n")

	// 1000ms / 5000 msg/sec = 0.2ms = 200 microseconds
	ticker := time.NewTicker(200 * time.Microsecond)
	statsTicker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	defer statsTicker.Stop()

	var lastTotalSent, lastTotalDone int64

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\n\n>>> Simulation Complete! Final Stats:")
			time.Sleep(2 * time.Second)
			printStats(p0Sent, p1Sent, p2Sent, p0Done, p1Done, p2Done, 0, 0)
			return
		case <-statsTicker.C:
			s0, s1, s2 := atomic.LoadInt64(&p0Sent), atomic.LoadInt64(&p1Sent), atomic.LoadInt64(&p2Sent)
			d0, d1, d2 := atomic.LoadInt64(&p0Done), atomic.LoadInt64(&p1Done), atomic.LoadInt64(&p2Done)
			
			currentTotalSent := s0 + s1 + s2
			currentTotalDone := d0 + d1 + d2
			
			publishRPS := currentTotalSent - lastTotalSent
			consumeRPS := currentTotalDone - lastTotalDone
			
			printStats(s0, s1, s2, d0, d1, d2, publishRPS, consumeRPS)
			
			lastTotalSent = currentTotalSent
			lastTotalDone = currentTotalDone
		case <-ticker.C:
			prio := rand.Intn(3)
			jobId := uuid.NewString()[:8]
			
			// Non-blocking send
			p.Send(context.Background(), &messaging.Message{
				Key:      "job-" + jobId,
				Priority: prio,
				Payload:  "5k-rps-payload",
			})

			switch prio {
			case 0: atomic.AddInt64(&p0Sent, 1)
			case 1: atomic.AddInt64(&p1Sent, 1)
			case 2: atomic.AddInt64(&p2Sent, 1)
			}
		}
	}
}

func printStats(s0, s1, s2, d0, d1, d2, pubRps, consRps int64) {
	fmt.Printf("\r[STATS] RPS: [Pub: %-5d, Cons: %-5d] | SENT: P0:%-3d P1:%-3d P2:%-3d | DONE: P0:%-3d P1:%-3d P2:%-3d",
		pubRps, consRps, s0, s1, s2, d0, d1, d2)
}
