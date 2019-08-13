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
	// OnShouldProcess is called before unmarshalling to allow the
	// consumer control over forwarding decisions
	OnShouldProcess func(o interface{}) bool
	// OnShouldFilter is called before forwarding to the consumer
	// to give the consumer control over filtering messages
	OnShouldFilter func(m *Message) bool
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
	cb.OnShouldProcess = nil
	cb.OnShouldFilter = nil
	cb.mu.Unlock()
	return nil
}

func (cb *ConsumerCallbackAdapter) ShouldProcess(o interface{}) bool {
	cb.mu.Lock()
	h := cb.OnShouldProcess
	cb.mu.Unlock()
	if h != nil {
		return h(o)
	}
	return true
}

func (cb *ConsumerCallbackAdapter) ShouldFilter(m *Message) bool {
	cb.mu.Lock()
	h := cb.OnShouldFilter
	cb.mu.Unlock()
	if h != nil {
		return h(m)
	}
	return true
}

// ConsumerCallback will receive events from producers
type ConsumerCallback interface {
	// OnDataReceived is called when an event is received
	DataReceived(msg Message) error
	// OnErrorReceived is called when an error is received
	ErrorReceived(err error)
}

// ConsumerCallbackMessageFilter is a filter for handling forwarding
// decisions after receiving from kafka before before unmarshalling
// NOTE: we explicitly use an object here which the implementer
// needs to cast as a *kafka.Message so as not to create an
// explicit dependency on the go confluent kafka library for this
// package where you don't need it (and that library is a bit of a
// pain because its Cgo and requires third-party library to compile)
type ConsumerCallbackMessageFilter interface {
	ShouldProcess(o interface{}) bool
}

// ConsumerCallbackEventFilter is a filter for handling forwarding
// decisions after deserialization from kafka before deliver to the
// consumer callback. return true to forward or false to stop
// processing
type ConsumerCallbackEventFilter interface {
	ShouldFilter(m *Message) bool
}

// Consumer will create a consumer for receiving events
type Consumer interface {
	// Close will stop listening for events
	Close() error
	// Consume will start consuming from the consumer using the callback
	Consume(callback ConsumerCallback)
}
