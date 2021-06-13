package messaging

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJsonPathFromMessage_String(t *testing.T) {
	event :=
		`
			{
				"id": "1234",
				"data": {
					"data_id": "345"
				}
			}
		`
	message := Message{
		Payload: event,
	}

	id, err := message.GetJsonPathAsString("$.id")
	assert.NoError(t, err)
	assert.Equal(t, "1234", id)

	id, err = message.GetJsonPathAsString("$.data.data_id")
	id, err = message.GetJsonPathAsString("$.data.data_id")
	assert.NoError(t, err)
	assert.Equal(t, "345", id)
}

func TestJsonPathFromMessage_Int(t *testing.T) {
	event :=
		`
			{
				"id": 1234,
				"data": {
					"data_id": 345
				}
			}
		`
	message := Message{
		Payload: event,
	}

	id, err := message.GetJsonPathAsString("$.id")
	assert.NoError(t, err)
	assert.Equal(t, "1234", id)

	id, err = message.GetJsonPathAsString("$.data.data_id")
	id, err = message.GetJsonPathAsString("$.data.data_id")
	assert.NoError(t, err)
	assert.Equal(t, "345", id)
}

func TestJsonPathFromMessage_Bool(t *testing.T) {
	event :=
		`
			{
				"id": true,
				"data": {
					"data_id": false
				}
			}
		`
	message := Message{
		Payload: event,
	}

	id, err := message.GetJsonPathAsString("$.id")
	assert.NoError(t, err)
	assert.Equal(t, "true", id)

	id, err = message.GetJsonPathAsString("$.data.data_id")
	id, err = message.GetJsonPathAsString("$.data.data_id")
	assert.NoError(t, err)
	assert.Equal(t, "false", id)
}
