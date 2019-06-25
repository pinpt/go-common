package eventing

import (
	"context"
	"time"

	"github.com/linkedin/goavro"
)

// ValueEncodingType is the type of encoding for the Value payload
type ValueEncodingType string

const (
	// JSONEncoding is json encoding for the Value payload
	JSONEncoding ValueEncodingType = "json"
	// AvroEncoding is avro encoding for the Value payload
	AvroEncoding ValueEncodingType = "avro"
)

// Message encapsulates data for the event system
type Message struct {
	Encoding  ValueEncodingType
	Codec     *goavro.Codec
	Key       string
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
	Extra     map[string]interface{}
	Topic     string
	Partition int32
}

// Producer will emit events to consumers
type Producer interface {
	// Send will send the event
	Send(ctx context.Context, msg Message) error
	// Close will close the producer
	Close() error
}

// ConsumerCallbackAdapter will receive events from producers
type ConsumerCallbackAdapter struct {
	// OnDataReceived is called when an event is received
	OnDataReceived func(msg Message) error
	// OnErrorReceived is called when an error is received
	OnErrorReceived func(err error)
	// OnEOF is called when a topic partition EOF is received
	OnEOF func(topic string, partition int32, offset int64)
	// OnStats is called when topic stats is generated
	OnStats func(stats map[string]interface{})
}

func (cb *ConsumerCallbackAdapter) DataReceived(msg Message) error {
	if cb.OnDataReceived != nil {
		return cb.OnDataReceived(msg)
	}
	return nil
}

func (cb *ConsumerCallbackAdapter) ErrorReceived(err error) {
	if cb.OnErrorReceived != nil {
		cb.OnErrorReceived(err)
	}
}

func (cb *ConsumerCallbackAdapter) EOF(topic string, partition int32, offset int64) {
	if cb.OnEOF != nil {
		cb.OnEOF(topic, partition, offset)
	}
}

func (cb *ConsumerCallbackAdapter) Stats(stats map[string]interface{}) {
	if cb.OnStats != nil {
		cb.OnStats(stats)
	}
}

// ConsumerCallback will receive events from producers
type ConsumerCallback interface {
	// OnDataReceived is called when an event is received
	DataReceived(msg Message) error
	// OnErrorReceived is called when an error is received
	ErrorReceived(err error)
}

// Consumer will create a consumer for receiving events
type Consumer interface {
	// Close will stop listening for events
	Close() error
	// Consume will start consuming from the consumer using the callback
	Consume(callback ConsumerCallback)
}
