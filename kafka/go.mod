module github.com/harishb2k/gox-messaging/kafka

go 1.15

replace github.com/harishb2k/gox-messaging => ../

require (
	github.com/confluentinc/confluent-kafka-go v1.5.2
	github.com/google/uuid v1.2.0
	github.com/harishb2k/gox-base v0.0.23
	github.com/harishb2k/gox-messaging v0.0.0-20210304093612-5136e0c1fdae
	github.com/pkg/errors v0.9.1
)
