package eventing

import (
	"context"
	"errors"
	"sync"
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

// ErrMessageNotAutoCommit is returned if you call commit on a message that isn't in auto commit mode
var ErrMessageNotAutoCommit = errors.New("message isn't auto commit")

type CommitOverride func(m Message) error

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
	Offset    int64

	// internal, do not set
	CommitOverride CommitOverride
	AutoCommit     bool
}

// IsAutoCommit returns true if the message is automatically commit or false if you must call Commit when completed
func (m Message) IsAutoCommit() bool {
	return m.AutoCommit
}

// Commit is used to commit the processing of this event and store the offset
func (m Message) Commit() error {
	if m.CommitOverride != nil {
		return m.CommitOverride(m)
	}
	return ErrMessageNotAutoCommit
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

	// mutex
	mu sync.Mutex
}

func (cb *ConsumerCallbackAdapter) DataReceived(msg Message) error {
	cb.mu.Lock()
	h := cb.OnDataReceived
	cb.mu.Unlock()
	if h != nil {
		return h(msg)
	}
	return nil
}

func (cb *ConsumerCallbackAdapter) ErrorReceived(err error) {
	cb.mu.Lock()
	h := cb.OnErrorReceived
	cb.mu.Unlock()
	if h != nil {
		h(err)
	}
}

func (cb *ConsumerCallbackAdapter) EOF(topic string, partition int32, offset int64) {
	cb.mu.Lock()
	h := cb.OnEOF
	cb.mu.Unlock()
	if h != nil {
		h(topic, partition, offset)
	}
}

func (cb *ConsumerCallbackAdapter) Stats(stats map[string]interface{}) {
	cb.mu.Lock()
	h := cb.OnStats
	cb.mu.Unlock()
	if h != nil {
		h(stats)
	}
}

func (cb *ConsumerCallbackAdapter) Close() error {
	cb.mu.Lock()
	cb.OnDataReceived = nil
	cb.OnEOF = nil
	cb.OnErrorReceived = nil
	cb.OnStats = nil
	cb.mu.Unlock()
	return nil
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
