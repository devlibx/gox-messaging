module github.com/devlibx/gox-messaging

go 1.16

replace (
	github.com/devlibx/gox-base => /Users/harishbohara/workspace/personal/gox/gox-base
)

require (
	github.com/aws/aws-sdk-go v1.38.39
	github.com/confluentinc/confluent-kafka-go v1.7.0
	github.com/devlibx/gox-aws v0.0.11
	github.com/devlibx/gox-base v0.0.48
	github.com/google/uuid v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	go.uber.org/zap v1.16.0
)
