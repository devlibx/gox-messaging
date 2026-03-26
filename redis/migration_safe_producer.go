package redis

import (
	"context"
	"fmt"
	"time"

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
	// Publish to primary topic (primary gets the full original context)
	resChan := m.primary.Send(ctx, message)

	// Publish to migration topic with a shorter timeout (e.g., 200ms)
	// We do this to ensure dual-delivery attempt without stalling primary for too long
	migrationCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	go func() {
		defer cancel()
		// We call Send and wait for its completion (or timeout)
		// Since we're in a goroutine, we don't block the primary return
		select {
		case <-m.migration.Send(migrationCtx, message):
			// Migration send finished
		case <-migrationCtx.Done():
			// Migration send timed out or parent context cancelled
		}
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
	// We use the SAME topic as the primary producer
	migrationEndpoint, _ := config.Properties["migration_endpoint"].(string)
	if migrationEndpoint == "" {
		primary.Stop()
		return nil, fmt.Errorf("redis migration enabled for (%s) - but migration_endpoint is missing in properties", config.Name)
	}

	// Create a new config for migration and override with migration specific properties
	migrationConfig := config
	migrationConfig.Endpoint = migrationEndpoint
	// Topic remains the same as primary: migrationConfig.Topic = config.Topic

	// Map migration_* properties to standard property names for the second producer
	migrationConfig.Properties = map[string]interface{}{}
	// Copy all original properties first to maintain settings like max_attempts etc.
	for k, v := range config.Properties {
		migrationConfig.Properties[k] = v
	}

	// Override with migration specific values
	if val, ok := config.Properties["migration_password"].(string); ok {
		migrationConfig.Properties["password"] = val
	}
	if val, ok := config.Properties["migration_tls_enabled"].(bool); ok {
		migrationConfig.Properties["tls_enabled"] = val
	}
	if val, ok := config.Properties["migration_cluster_mode"].(bool); ok {
		migrationConfig.Properties["cluster_mode"] = val
	}
	if val, ok := config.Properties["migration_mandatory_service_name"].(string); ok {
		migrationConfig.MandatoryServiceName = val
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
