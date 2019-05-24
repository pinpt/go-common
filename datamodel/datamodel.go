package datamodel

import (
	"context"

	"github.com/linkedin/goavro"
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

// Model is a generic model interface that all our models implement
type Model interface {
	// GetID returns the ID for the instance
	GetID() string
	// GetAvroCodec returns the avro codec for this model
	GetAvroCodec() *goavro.Codec
	// ToAvroBinary converts the instance to binary avro
	ToAvroBinary() ([]byte, *goavro.Codec, error)
	// Stringfy converts the instance to JSON string
	Stringify() string
	// ToMap converts the instance to a map
	ToMap(avro ...bool) map[string]interface{}
	// FromMap sets the properties of the instance from the map
	FromMap(kv map[string]interface{})
	// IsMaterialized returns true if the model is materialized
	IsMaterialized() bool
	// MaterializedName returns the name of the materialized table
	MaterializedName() string
}

// Db is an interface to the database
type Db interface {
	Create(ctx context.Context, kv map[string]interface{}) error
	Update(ctx context.Context, id string, kv map[string]interface{}) error
	Delete(ctx context.Context, id string) error
	FindOne(ctx context.Context, id string) (map[string]interface{}, error)
	Find(ctx context.Context, kv map[string]interface{}) ([]map[string]interface{}, error)
}

// Producer will emit events to consumers
type Producer interface {
	// Send will send the event
	Send(ctx context.Context, codec *goavro.Codec, key []byte, value []byte) error
	// Close will close the producer
	Close() error
}

// ConsumerCallback will receive events from producers
type ConsumerCallback struct {
	// OnDataReceived is called when an event is received
	OnDataReceived func(key []byte, value []byte) error
	// OnErrorReceived is called when an error is received
	OnErrorReceived func(err error)
}

// Consumer will create a consumer for receiving events
type Consumer interface {
	// Close will stop listening for events
	Close() error
}

// ConsumerFactory is for creating consumers
type ConsumerFactory interface {
	// CreateConsumer will create a new consumer for a given topic and callback to handle events
	CreateConsumer(topic string, callback ConsumerCallback) (Consumer, error)
}
