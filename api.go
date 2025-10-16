package messaging

import (
	"context"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	"github.com/devlibx/gox-base/v2/serialization"
	"github.com/yalp/jsonpath"
	"go.uber.org/zap"
	"time"
)

//go:generate mockgen -source=api.go -destination=./mocks/mock_api.go -package=mockMessaging

var ErrProducerNotFound = errors.New("produce not found")
var ErrProducerClosed = errors.New("produce is closed")
var ErrConsumerNotFound = errors.New("consumer not found")

const (
	KMessagingPropertyTopic                          = "topic"
	KMessagingPropertyEndpoint                       = "endpoint"
	KMessagingPropertyGroupId                        = "group.id"
	KMessagingPropertyConcurrency                    = "concurrency"
	KMessagingPropertyAutoOffsetReset                = "auto.offset.reset"
	KMessagingPropertyEnableAutoCommit               = "enable.auto.commit"
	KMessagingPropertyAcks                           = "acks"
	KMessagingPropertySessionTimeoutMs               = "session.timeout.ms"
	KMessagingPropertyPublishMessageTimeoutMs        = "publish.message.timeout.ms"
	KMessagingPropertyWaitAssignment                 = "wait.assignment"
	KMessagingPropertyReplicationFactor              = "replication_factor"
	KMessagingPropertyPartitions                     = "partitions"
	KMessagingPropertyBrokers                        = "brokers"
	KMessagingPropertyDisableDeliveryReports         = "go.delivery.reports"
	KMessagingPropertyLingerMs                       = "linger.ms"
	KMessagingPropertyBatchSize                      = "batch.size"
	KMessagingPropertyBufferMemory                   = "buffer.memory"
	KMessagingPropertyCompressionType                = "compression.type"
	KMessagingPropertyRateLimitPerSec                = "rate_limit_per_sec"
	KMessagingPropertyPartitionProcessingParallelism = "partition_processing_parallelism"
	KMessagingPropertyErrorReportingChannelSize      = "error_reporting_channel_size"

	KMessagingPropertyMigrationEnabled  = "migration_enabled"
	KMessagingPropertyMigrationTopic    = "migration_topic"
	KMessagingPropertyMigrationEndpoint = "migration_endpoint"
)

// Ignorable is an interface which can be implemented by someone if they want to be ignored
//
// # NOTE - if IsIgnorable() returns true then ErrorInProcessing method will not be called on error function
//
// One of the example is an error reporter by SQS consumer which can be ignored
// e.g. when SQS consumer sends an error, we do not delete it, and it will be retried after some time.
// However, if client knows that this error is not ignorable then client can implement this interface -
// if implemented and IsIgnorable() returns true then we will delete the method from SQS, and it will not be retried.
type Ignorable interface {
	IsIgnorable() bool
}

// Provides producer and consumers
type Factory interface {
	MarkStart()
	Start(configuration Configuration) error
	GetProducer(name string) (Producer, error)
	GetConsumer(name string) (Consumer, error)
	RegisterProducer(config ProducerConfig) error
	RegisterConsumer(config ConsumerConfig) error
	Stop() error
}

type KafkaMessageInfo struct {
	TopicPartition kafka.TopicPartition
}

// Message for consumption
type Message struct {
	Key                              string
	Payload                          interface{}
	MessageDelayInMs                 int
	ArtificialDelayToSimulateLatency time.Duration
	parsedJson                       interface{}
	KafkaMessageInfo                 KafkaMessageInfo

	SqsMessageDeduplicationId string
	SqsMessageGroupId         string
}

func (m *Message) PayloadAsString() (string, error) {
	if m.Payload == nil {
		return "", nil
	} else if data, ok := m.Payload.(string); ok {
		return data, nil
	} else if data, ok := m.Payload.([]byte); ok {
		return string(data), nil
	} else if data, err := serialization.Stringify(m.Payload); err != nil {
		return "", errors.Wrap(err, "failed to creat string from message: request=%v", m)
	} else {
		return data, nil
	}
}

func (m *Message) PayloadAsBytes() ([]byte, error) {
	str, err := m.PayloadAsString()
	if err != nil {
		return nil, err
	} else {
		return []byte(str), nil
	}
}

func (m *Message) PayloadAsStringObjectMap() (gox.StringObjectMap, error) {
	if str, err := m.PayloadAsString(); err == nil {
		m := gox.StringObjectMap{}
		if err = serialization.JsonBytesToObject([]byte(str), &m); err == nil {
			return m, nil
		}
		return nil, err
	} else {
		return nil, err
	}
}

func (ki *KafkaMessageInfo) GetPartitionId() int32 {
	return ki.TopicPartition.Partition
}

func (m *Message) GetPartitionId() int32 {
	return m.KafkaMessageInfo.TopicPartition.Partition
}

type Response struct {
	RawPayload interface{}
	Err        error
}

// Provides client with a capability to produce a message
type Producer interface {
	Send(ctx context.Context, message *Message) chan *Response
	Stop() error
}

// ErrorReporter is an interface which is implemented for specific use case. E.g. a Kafka async produce can be a
// ErrorReporter
type ErrorReporter interface {
	GetErrorReport() (chan *Response, bool, error)
}

type ConsumeFunc func(message *Message) error

// Consumer function which is called for each message
type Consumer interface {
	Process(ctx context.Context, consumeFunction ConsumeFunction) error
	Stop() error
}

type ConsumeFunction interface {
	Process(message *Message) error
	ErrorInProcessing(message *Message, err error)
}

type DefaultMessageChannelConsumeFunction struct {
	MessagesChannel chan *Message
	logger          *zap.Logger
	gox.CrossFunction
}

func (n *DefaultMessageChannelConsumeFunction) Process(message *Message) error {
	n.logger.Debug("got message in [1]:", zap.Any("payload", message.Payload))
	n.MessagesChannel <- message
	return nil
}

func (n *DefaultMessageChannelConsumeFunction) ErrorInProcessing(message *Message, err error) {
	n.logger.Debug("failed to process message", zap.Any("message", message))
}

func NewDefaultMessageChannelConsumeFunction(cf gox.CrossFunction) *DefaultMessageChannelConsumeFunction {
	c := &DefaultMessageChannelConsumeFunction{
		CrossFunction:   cf,
		logger:          cf.Logger(),
		MessagesChannel: make(chan *Message, 1000),
	}
	return c
}

// --------------------------------------- Consumer function with process and error method -----------------------------
type simpleConsumeFunction struct {
	name   string
	logger *zap.Logger
	gox.CrossFunction
	ProcessFunc           func(message *Message) error
	ErrorInProcessingFunc func(message *Message, err error)
}

func (c *simpleConsumeFunction) Process(message *Message) error {
	if str, err := message.PayloadAsString(); err == nil {
		c.logger.Debug("<< [message in]", zap.String("key", message.Key), zap.String("payload", str))
	} else {
		c.logger.Debug("<< [message in]", zap.String("key", message.Key), zap.Any("payload", message.Payload))
	}
	if c.ProcessFunc != nil {
		return c.ProcessFunc(message)
	}
	return errors.New("process function not registered")
}
func (c *simpleConsumeFunction) ErrorInProcessing(message *Message, err error) {
	if c.ErrorInProcessingFunc != nil {
		c.ErrorInProcessingFunc(message, err)
	}
}

// Create a simple consumer function
func NewSimpleConsumeFunction(cf gox.CrossFunction, name string, processF func(message *Message) error, errFunc func(message *Message, err error)) ConsumeFunction {
	return &simpleConsumeFunction{
		name:                  name,
		logger:                cf.Logger().Named("consume_func").Named(name),
		ProcessFunc:           processF,
		ErrorInProcessingFunc: errFunc,
	}
}

func (m *Message) GetJsonPath(path string) (interface{}, error) {
	if m.parsedJson != nil {
		if result, err := jsonpath.Read(m.parsedJson, path); err == nil {
			return result, nil
		}
	} else {
		if b, err := m.PayloadAsBytes(); err == nil {
			if err := json.Unmarshal(b, &m.parsedJson); err == nil {
				if result, err := jsonpath.Read(m.parsedJson, path); err == nil {
					return result, nil
				}
			}
		}
	}
	return "", errors.New("json path not found: path=%s", path)
}

func (m *Message) GetJsonPathAsString(path string) (string, error) {
	if result, err := m.GetJsonPath(path); err == nil {
		return serialization.Stringify(result)
	} else {
		return "", err
	}
}
