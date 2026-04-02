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

	// 4. Start Consumer
	c, _ := f.GetConsumer("priority_consumer")
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
			// Small artificial delay to simulate work and let queue build up
			time.Sleep(50 * time.Millisecond)
			return nil
		},
		func(message *messaging.Message, err error) {},
	)
	_ = c.Process(context.Background(), consumeFunc)

	// 5. Start Producer Loop (2 Minutes)
	p, _ := f.GetProducer("priority_producer")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	fmt.Println(">>> Starting 2-minute Priority Queue Simulation...")
	fmt.Println(">>> We will submit random jobs (P0, P1, P2) and watch them get processed.")

	ticker := time.NewTicker(200 * time.Millisecond)
	statsTicker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	defer statsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\n>>> Time's up! Waiting for final jobs to clear...")
			time.Sleep(5 * time.Second)
			printStats(p0Sent, p1Sent, p2Sent, p0Done, p1Done, p2Done)
			return
		case <-statsTicker.C:
			printStats(p0Sent, p1Sent, p2Sent, p0Done, p1Done, p2Done)
		case <-ticker.C:
			// Send a job with random priority
			prio := rand.Intn(3) // 0, 1, or 2
			jobId := uuid.NewString()[:8]
			
			p.Send(context.Background(), &messaging.Message{
				Key:      "job-" + jobId,
				Priority: prio,
				Payload:  "data",
			})

			switch prio {
			case 0:
				atomic.AddInt64(&p0Sent, 1)
			case 1:
				atomic.AddInt64(&p1Sent, 1)
			case 2:
				atomic.AddInt64(&p2Sent, 1)
			}
		}
	}
}

func printStats(p0S, p1S, p2S, p0D, p1D, p2D int64) {
	s0, s1, s2 := atomic.LoadInt64(&p0S), atomic.LoadInt64(&p1S), atomic.LoadInt64(&p2S)
	d0, d1, d2 := atomic.LoadInt64(&p0D), atomic.LoadInt64(&p1D), atomic.LoadInt64(&p2D)

	fmt.Printf("\r[STATS] SENT: P0:%-3d P1:%-3d P2:%-3d | DONE: P0:%-3d P1:%-3d P2:%-3d",
		s0, s1, s2, d0, d1, d2)
}
