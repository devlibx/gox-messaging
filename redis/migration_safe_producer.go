package redis

import (
	"context"
	"fmt"

	"github.com/devlibx/gox-base/v2"
	messaging "github.com/devlibx/gox-messaging/v2"
)

// migrationSafeRedisProducer implements the messaging.Producer interface
// as a decorator that wraps a primary and a migration producer.
type migrationSafeRedisProducer struct {
	primary   messaging.Producer
	migration messaging.Producer
}

func (m *migrationSafeRedisProducer) Send(ctx context.Context, message *messaging.Message) chan *messaging.Response {
	// Publish to primary topic
	resChan := m.primary.Send(ctx, message)

	// Publish to migration topic in background to ensure zero impact on primary latency
	go func() {
		m.migration.Send(ctx, message)
	}()

	return resChan
}

func (m *migrationSafeRedisProducer) Stop() error {
	err1 := m.primary.Stop()
	err2 := m.migration.Stop()
	if err1 != nil {
		return err1
	}
	return err2
}

// NewMigrationSafeRedisProducer creates a composite producer that manages dual-publishing
// for migration scenarios using a decorator pattern.
func NewMigrationSafeRedisProducer(cf gox.CrossFunction, config messaging.ProducerConfig) (messaging.Producer, error) {
	// 1. Create primary producer
	primary, err := NewRedisProducer(cf, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create primary redis producer: %w", err)
	}

	// 2. Setup configuration for the migration producer
	migrationConfig := config
	migrationConfig.Endpoint = config.MigrationEndpoint
	migrationConfig.Topic = config.MigrationTopic

	// Ensure migration-specific properties (auth, TLS, cluster_mode) are correctly merged
	if migrationConfig.Properties == nil {
		migrationConfig.Properties = map[string]interface{}{}
	}
	if config.MigrationProperties != nil {
		for k, v := range config.MigrationProperties {
			migrationConfig.Properties[k] = v
		}
	}

	// 3. Create migration producer
	migration, err := NewRedisProducer(cf, migrationConfig)
	if err != nil {
		// Clean up primary if migration fails to start
		primary.Stop()
		return nil, fmt.Errorf("failed to create migration redis producer: %w", err)
	}

	return &migrationSafeRedisProducer{
		primary:   primary,
		migration: migration,
	}, nil
}
