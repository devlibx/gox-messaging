package redis

import (
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

func GetRedisClientKey(endpoint string, properties map[string]interface{}) string {
	db := 0
	if val, ok := properties["db"].(int); ok {
		db = val
	} else if val, ok := properties["db"].(float64); ok {
		db = int(val)
	}

	clusterMode := false
	if val, ok := properties["cluster_mode"].(bool); ok {
		clusterMode = val
	}

	tlsEnabled := false
	if val, ok := properties["tls_enabled"].(bool); ok {
		tlsEnabled = val
	}

	user := ""
	if val, ok := properties["user"].(string); ok {
		user = val
	}

	password := ""
	if val, ok := properties["password"].(string); ok {
		password = val
	}

	readTimeout := 0
	if val, ok := properties["read_timeout_ms"].(int); ok {
		readTimeout = val
	}

	writeTimeout := 0
	if val, ok := properties["write_timeout_ms"].(int); ok {
		writeTimeout = val
	}

	putTimeout := 0
	if val, ok := properties["put_timeout_ms"].(int); ok {
		putTimeout = val
	}

	getTimeout := 10
	if val, ok := properties["get_timeout_ms"].(int); ok {
		getTimeout = val
	}

	return fmt.Sprintf("endpoint=%s__db=%d__cluster_mode=%v__tls_enabled=%v__user=%s__password=%s__read_timeout=%d__write_timeout=%d__put_timeout=%d__get_timeout=%d",
		endpoint, db, clusterMode, tlsEnabled, user, password, readTimeout, writeTimeout, putTimeout, getTimeout)
}

func CreateRedisUniversalClient(endpoint string, properties map[string]interface{}) (redis.UniversalClient, error) {
	addrs := strings.Split(endpoint, ",")
	opt := &redis.UniversalOptions{
		Addrs: addrs,
	}

	if val, ok := properties["password"].(string); ok {
		opt.Password = val
	}

	if val, ok := properties["db"].(int); ok {
		opt.DB = val
	} else if val, ok := properties["db"].(float64); ok {
		opt.DB = int(val)
	}

	if val, ok := properties["tls_enabled"].(bool); ok && val {
		opt.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	var client redis.UniversalClient
	isCluster, _ := properties["cluster_mode"].(bool)
	if isCluster {
		client = redis.NewClusterClient(opt.Cluster())
	} else {
		client = redis.NewUniversalClient(opt)
	}
	return client, nil
}
