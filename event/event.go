package event

import (
	"context"
	"time"

	"github.com/linkedin/goavro"
)

// Message encapsulates data for the event system
type Message struct {
	Codec     *goavro.Codec
	Key       string
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
	Extra     map[string]interface{}
}

// Producer will emit events to consumers
type Producer interface {
	// Send will send the event
	Send(ctx context.Context, msg Message) error
	// Close will close the producer
	Close() error
}

// ConsumerCallback will receive events from producers
type ConsumerCallback struct {
	// OnDataReceived is called when an event is received
	OnDataReceived func(msg Message) error
	// OnErrorReceived is called when an error is received
	OnErrorReceived func(err error)
}

// Consumer will create a consumer for receiving events
type Consumer interface {
	// Close will stop listening for events
	Close() error
	// Consume will start consuming from the consumer using the callback
	Consume(callback ConsumerCallback)
}
