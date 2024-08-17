package factory

import (
	"context"
	messaging "github.com/devlibx/gox-messaging/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNoOpFactory(t *testing.T) {
	mf := NewNoOpMessagingFactory()
	err := mf.Start(messaging.Configuration{})
	assert.NoError(t, err)

	p, err := mf.GetProducer("dummy")
	assert.NoError(t, err)
	res := p.Send(context.Background(), &messaging.Message{Key: "user_1", Payload: "1234"})
	assert.NotNil(t, res)
	assert.NotNil(t, <-res)
}
