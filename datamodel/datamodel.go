package datamodel

import (
	"time"

	"github.com/pinpt/go-common/eventing"
)

// TopicNameType is a type for the name of a topic
type TopicNameType string

// String returns the value as a string
func (t TopicNameType) String() string {
	return string(t)
}

// ModelNameType is a type for the model name
type ModelNameType string

// String returns the value as a string
func (t ModelNameType) String() string {
	return string(t)
}

// CleanupPolicy is the constant for cleanup policies supported on the topic
type CleanupPolicy string

const (
	// DeleteCleanupPolicy will delete records older than Retention
	DeleteCleanupPolicy CleanupPolicy = "delete"
	// CompactCleanupPolicy will compact records older than Retention
	CompactCleanupPolicy CleanupPolicy = "compact"
	// DefaultCleanupPolicy is the default policy which is CompactCleanupPolicy
	DefaultCleanupPolicy CleanupPolicy = CompactCleanupPolicy
)

// ModelTopicConfig is a configuration for the topic
type ModelTopicConfig struct {
	// NumPartitions returns the number of partitions for the topic
	NumPartitions int
	// ReplicationFactor returns the replication factor for the topic
	ReplicationFactor int
	// Retention returns the retention duration for items in the topic
	Retention time.Duration
	// MaxSize in bytes for items in the topic
	MaxSize int64
	// Key is the message key field for the topic
	Key string
	// Timestamp is the timestamp field for the topic
	Timestamp string
	// TTL is the duration the message is valid
	TTL time.Duration
	// CleanupPolicy is the policy for how to deal with older records
	CleanupPolicy CleanupPolicy
}

// ModelMaterializeConfig is a configuration for the materialization
type ModelMaterializeConfig struct {
	// KeyName is the name of the key field
	KeyName string
	// TableName is the name of the table to materialize
	TableName string
	// IdleTime returns the idle time before committing the data
	IdleTime time.Duration
	// BatchSize returns the number of records to batch before materializing
	BatchSize int
}

// Model is a generic model interface that all our models implement
type Model interface {
	// Clone returns an exact copy of the model
	Clone() Model
	// Anon returns the model with the sensitive fields anonymized
	Anon() Model
	// GetID returns the ID for the instance
	GetID() string
	// Stringfy converts the instance to JSON string
	Stringify() string
	// ToMap converts the instance to a map
	ToMap() map[string]interface{}
	// FromMap sets the properties of the instance from the map
	FromMap(kv map[string]interface{})
	// GetModelName returns the name of the model
	GetModelName() ModelNameType
	// IsMaterialized returns true if the model is materialized
	IsMaterialized() bool
	// IsMutable returns true if the model is mutable
	IsMutable() bool
	// GetModelMaterializeConfig returns the materialization config if materialized or nil if not
	GetModelMaterializeConfig() *ModelMaterializeConfig
	// IsEvented returns true if the model supports eventing and implements ModelEventProvider
	IsEvented() bool
	// GetTopicName returns the name of the topic if evented or nil if not
	GetTopicName() TopicNameType
	// GetTopicConfig returns the topic config object
	GetTopicConfig() *ModelTopicConfig
	// GetTopicKey returns the topic message key when sending this model as a ModelSendEvent
	GetTopicKey() string
	// GetStreamName returns the name of the stream if evented or "" if not
	GetStreamName() string
	// GetTableName returns the name of the table if evented or "" if not
	GetTableName() string
	// GetTimestamp returns the timestamp for the model or now if not provided
	GetTimestamp() time.Time
}

// StreamedModel is a model that is streamed
type StreamedModel interface {
	// FIXME: re-breakout the model (public) from stream model (private) once things settle
	Model
}

// ModelReceiveEvent is a model event received on an event consumer channel
type ModelReceiveEvent interface {
	// Object returns an instance of the Model that was received
	Object() StreamedModel
	// Message returns the underlying message data for the event
	Message() eventing.Message
	// EOF returns true if an EOF event was received. in this case, the Object and Message will return nil
	EOF() bool
}

// ModelSendEvent is a model event to send on an event producer channel
type ModelSendEvent interface {
	// Key is the key to use for the message
	Key() string
	// Object returns an instance of the Model that will be send
	Object() StreamedModel
	// Headers returns any headers for the event. can be nil to not send any additional headers
	Headers() map[string]string
	// Timestamp returns the event timestamp. If empty, will default to time.Now()
	Timestamp() time.Time
}

// PartialModel is a generic model interface that all our models implement
type PartialModel interface {
	// Stringfy converts the instance to JSON string
	Stringify() string
	// ToMap converts the instance to a map
	ToMap() map[string]interface{}
	// GetModelName returns the name of the model
	GetModelName() ModelNameType
}
