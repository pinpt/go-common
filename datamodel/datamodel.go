package datamodel

import "github.com/linkedin/goavro"

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
