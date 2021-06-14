package main

import (
	"github.com/devlibx/gox-base"
	"go.uber.org/zap"
)

func main() {
	zapConfig := zap.NewDevelopmentConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	cf := gox.NewCrossFunction(zapConfig.Build())

	// Send SQS message
	err := SqsSendMessage(cf)
	if err != nil {
		panic(err)
	}
}
