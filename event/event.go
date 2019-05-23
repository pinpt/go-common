package event

import (
	"context"

	"github.com/linkedin/goavro"
)

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
