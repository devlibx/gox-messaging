package redis

import (
	"context"
	"fmt"

	"github.com/devlibx/gox-base/v2"
	messaging "github.com/devlibx/gox-messaging/v2"
)

// migrationSafeRedisConsumer implements the messaging.Consumer interface
// as a decorator that wraps a primary and a migration consumer.
type migrationSafeRedisConsumer struct {
	primary   messaging.Consumer
	migration messaging.Consumer
}

func (m *migrationSafeRedisConsumer) Process(ctx context.Context, consumeFunction messaging.ConsumeFunction) error {
	// Start both consumers concurrently to process messages from both environments
	err1 := m.primary.Process(ctx, consumeFunction)
	err2 := m.migration.Process(ctx, consumeFunction)
	if err1 != nil {
		return err1
	}
	return err2
}

func (m *migrationSafeRedisConsumer) Stop() error {
	err1 := m.primary.Stop()
	err2 := m.migration.Stop()
	if err1 != nil {
		return err1
	}
	return err2
}

// NewMigrationSafeRedisConsumer creates a composite consumer that manages dual-consumption
// for migration scenarios using a decorator pattern.
func NewMigrationSafeRedisConsumer(cf gox.CrossFunction, config messaging.ConsumerConfig) (messaging.Consumer, error) {
	// 1. Create primary consumer
	primary, err := NewRedisConsumer(cf, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create primary redis consumer: %w", err)
	}

	// 2. Setup configuration for the migration consumer
	// Extract migration endpoint from properties to keep top-level clean
	migrationEndpoint, _ := config.Properties["migration_endpoint"].(string)
	if migrationEndpoint == "" {
		primary.Stop()
		return nil, fmt.Errorf("redis migration enabled for (%s) - but migration_endpoint is missing in properties", config.Name)
	}

	// Create a DEEP COPY of config for migration and override with migration specific properties
	migrationConfig := config
	migrationConfig.Properties = gox.StringObjectMap{}
	for k, v := range config.Properties {
		migrationConfig.Properties[k] = v
	}
	migrationConfig.Endpoint = migrationEndpoint

	// Override with migration specific values from Properties map
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
	if val, ok := config.Properties["migration_db"].(int); ok {
		migrationConfig.Properties["db"] = val
	} else if val, ok := config.Properties["migration_db"].(float64); ok {
		migrationConfig.Properties["db"] = int(val)
	}

	// Handle migration specific properties from the "migration_properties" sub-map if present
	if migProps, ok := config.Properties["migration_properties"].(map[string]interface{}); ok {
		for k, v := range migProps {
			migrationConfig.Properties[k] = v
			// Also support top-level fields if they are in the sub-map
			if k == "mandatory_service_name" {
				migrationConfig.MandatoryServiceName = v.(string)
			}
		}
	}

	// 3. Create migration consumer
	migration, err := NewRedisConsumer(cf, migrationConfig)
	if err != nil {
		// Clean up primary if migration fails to start
		primary.Stop()
		return nil, fmt.Errorf("failed to create migration redis consumer: %w", err)
	}

	return &migrationSafeRedisConsumer{
		primary:   primary,
		migration: migration,
	}, nil
}
