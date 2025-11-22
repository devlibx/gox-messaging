package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/base64"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	"google.golang.org/api/option"
)

func buildPubSubClient(ok bool, config gox.StringObjectMap, project string) (*pubsub.Client, error) {

	// We may use json service account credentials to connect
	var err error
	jsonCredentials, ok := config["json_credentials"].(string)
	var decodedJsonCredentials []byte
	if ok && jsonCredentials != "" {
		if decodedJsonCredentials, err = base64.StdEncoding.DecodeString(jsonCredentials); err != nil {
			return nil, errors.Wrap(err, "failed to decode json credentials from base64")
		}
	}

	// Create a new pubsub client
	var client *pubsub.Client
	if decodedJsonCredentials != nil && len(decodedJsonCredentials) > 0 {
		client, err = pubsub.NewClient(context.Background(), project, option.WithCredentialsJSON(decodedJsonCredentials))
	} else {
		client, err = pubsub.NewClient(context.Background(), project)
	}

	return client, err
}
