package sqs

import (
	"flag"
	goxAws "github.com/devlibx/gox-aws"
	"github.com/devlibx/gox-aws/messaging"
	"github.com/devlibx/gox-base/test"
	"github.com/devlibx/gox-base/util"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
)

var queue string

func init() {
	flag.StringVar(&queue, "real.sqs.queue", "", "Sqs queue to ues for testing")
}

func TestSqsSend(t *testing.T) {
	if util.IsStringEmpty(queue) {
		t.Skip("Need to pass SQS Queue using -real.sqs.queue=<name>")
	}

	cf, _ := test.MockCf(t, zap.DebugLevel)
	ctx, err := goxAws.NewAwsContext(cf, goxAws.Config{})
	assert.NoError(t, err)

	producer := NewSqsProducer(cf, ctx, Config{QueueUrl: queue})
	response, err := producer.Send(&messaging.Event{
		Key:   "key",
		Value: map[string]interface{}{"key": "value"},
	})
	assert.NoError(t, err)
	assert.NotNil(t, response)
	cf.Logger().Debug("Output from SQS", zap.Any("sqsResponse", response))
}
