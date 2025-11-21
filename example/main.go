package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/devlibx/gox-base/v2"
	"go.uber.org/zap"
)

func main() {
	var example string
	flag.StringVar(&example, "example", "kafka", "Example to run (kafka, sqs, pubsub)")
	flag.Parse()

	zapConfig := zap.NewDevelopmentConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	z, _ := zapConfig.Build()
	cf := gox.NewCrossFunction(z)

	var err error
	switch example {
	case "kafka":
		err = KafkaSendMessage(cf)
	case "sqs":
		err = SqsSendMessage(cf)
	case "pubsub":
		err = PubSubSendMessage(cf)
	default:
		fmt.Printf("Unknown example: %s\n", example)
		os.Exit(1)
	}

	if err != nil {
		panic(err)
	}
	time.Sleep(2 * time.Second)
}