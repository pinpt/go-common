package datamodel

import (
	"context"
	"time"

	"github.com/linkedin/goavro"
	"github.com/pinpt/go-common/event"
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
	// IsEvented returns true if the model supports eventing and implements ModelEventProvider
	IsEvented() bool
}

// Storage is an interface to storage model. It could be a data or a filesystem or an in memory cache
type Storage interface {
	// Create a new Model instance in the storage system
	Create(ctx context.Context, model Model) error
	// Update the model in the storage system
	Update(ctx context.Context, model Model) error
	// Delete the model from the storage system
	Delete(ctx context.Context, model Model) error
	// FindOne will find a model by the id and return. will be nil if not found
	FindOne(ctx context.Context, id string) (Model, error)
	// Find will query models in the storage system using the query and return an array of models. If none found, will be nil
	Find(ctx context.Context, query map[string]interface{}) ([]Model, error)
}

// ModelReceiveEvent is a model event received on an event consumer channel
type ModelReceiveEvent interface {
	// Object returns an instance of the Model that was received
	Object() Model
	// Message returns the underlying message data for the event
	Message() event.Message
}

// ModelSendEvent is a model event to send on an event producer channel
type ModelSendEvent interface {
	// Key is the key to use for the message
	Key() string
	// Object returns an instance of the Model that will be send
	Object() Model
	// Headers returns any headers for the event. can be nil to not send any additional headers
	Headers() map[string]string
	// Timestamp returns the event timestamp. If empty, will default to time.Now()
	Timestamp() time.Time
}

// ModelEventProducer is the producer interface
type ModelEventProducer interface {
	// Channel returns the producer channel to produce new events
	Channel() chan<- ModelSendEvent
	// Close is called to shutdown the producer
	Close() error
}

// ModelEventConsumer is the producer interface
type ModelEventConsumer interface {
	Channel() <-chan ModelReceiveEvent
	// Close is called to shutdown the producer
	Close() error
}

// ModelEventProvider is an interface that Models implement if they can send and receive events
type ModelEventProvider interface {
	// NewProducerChannel returns a channel which can be used for producing Model events
	NewProducerChannel(producer event.Producer, errors chan<- error) ModelEventProducer
	// NewConsumerChannel returns a consumer channel which can be used to consume Model events
	NewConsumerChannel(consumer event.Consumer, errors chan<- error) ModelEventConsumer
}
