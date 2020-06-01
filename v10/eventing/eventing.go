package eventing

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ValueEncodingType is the type of encoding for the Value payload
type ValueEncodingType string

const (
	// JSONEncoding is json encoding for the Value payload
	JSONEncoding ValueEncodingType = "json"
)

// ErrMessageNotAutoCommit is returned if you call commit on a message that isn't in auto commit mode
var ErrMessageNotAutoCommit = errors.New("message isn't auto commit")

type CommitOverride func(m Message) error

// Message encapsulates data for the event system
type Message struct {
	Encoding  ValueEncodingType
	Key       string
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
	Extra     map[string]interface{}
	Topic     string
	Partition int32
	Offset    int64

	// internal, do not set
	CommitOverride CommitOverride `json:"-"`
	AutoCommit     bool           `json:"-"`
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
	// OnPartitionAssignment is called when partitions are assigned to the consumer
	OnPartitionAssignment func(partitions []TopicPartition)
	// OnPartitionRevocation is called when partitions are unassigned to the consumer
	OnPartitionRevocation func(partitions []TopicPartition)
	// OnOffsetsCommitted is called when offsets are committed
	OnOffsetsCommitted func(offsets []TopicPartition)
	// mutex
	mu sync.RWMutex
}

var _ ConsumerCallback = (*ConsumerCallbackAdapter)(nil)
var _ ConsumerCallbackPartitionLifecycle = (*ConsumerCallbackAdapter)(nil)
var _ ConsumerCallbackMessageFilter = (*ConsumerCallbackAdapter)(nil)
var _ ConsumerCallbackEventFilter = (*ConsumerCallbackAdapter)(nil)

func (cb *ConsumerCallbackAdapter) DataReceived(msg Message) (err error) {
	cb.mu.RLock()
	if cb.OnDataReceived != nil {
		err = cb.OnDataReceived(msg)
	}
	cb.mu.RUnlock()
	return
}

func (cb *ConsumerCallbackAdapter) ErrorReceived(err error) {
	cb.mu.RLock()
	if cb.OnErrorReceived != nil {
		cb.OnErrorReceived(err)
	}
	cb.mu.RUnlock()
}

func (cb *ConsumerCallbackAdapter) EOF(topic string, partition int32, offset int64) {
	cb.mu.RLock()
	if cb.OnEOF != nil {
		cb.OnEOF(topic, partition, offset)
	}
	cb.mu.RUnlock()
}

func (cb *ConsumerCallbackAdapter) Stats(stats map[string]interface{}) {
	cb.mu.RLock()
	if cb.OnStats != nil {
		cb.OnStats(stats)
	}
	cb.mu.RUnlock()
}

func (cb *ConsumerCallbackAdapter) Close() error {
	cb.mu.Lock()
	cb.OnDataReceived = nil
	cb.OnEOF = nil
	cb.OnErrorReceived = nil
	cb.OnStats = nil
	cb.OnShouldProcess = nil
	cb.OnShouldFilter = nil
	cb.OnPartitionAssignment = nil
	cb.OnPartitionRevocation = nil
	cb.OnOffsetsCommitted = nil
	cb.mu.Unlock()
	return nil
}

func (cb *ConsumerCallbackAdapter) ShouldProcess(o interface{}) bool {
	cb.mu.RLock()
	ok := true
	if cb.OnShouldProcess != nil {
		ok = cb.OnShouldProcess(o)
	}
	cb.mu.RUnlock()
	return ok
}

func (cb *ConsumerCallbackAdapter) ShouldFilter(m *Message) bool {
	cb.mu.RLock()
	ok := true
	if cb.OnShouldFilter != nil {
		ok = cb.OnShouldFilter(m)
	}
	cb.mu.RUnlock()
	return ok
}

// OnPartitionAssignment is called when partitions are assigned to the consumer
func (cb *ConsumerCallbackAdapter) PartitionAssignment(partitions []TopicPartition) {
	cb.mu.RLock()
	if cb.OnPartitionAssignment != nil {
		cb.OnPartitionAssignment(partitions)
	}
	cb.mu.RUnlock()
}

// OnPartitionRevocation is called when partitions are unassigned to the consumer
func (cb *ConsumerCallbackAdapter) PartitionRevocation(partitions []TopicPartition) {
	cb.mu.RLock()
	if cb.OnPartitionRevocation != nil {
		cb.OnPartitionRevocation(partitions)
	}
	cb.mu.RUnlock()
}

// OnOffsetsCommitted is called when offsets are committed
func (cb *ConsumerCallbackAdapter) OffsetsCommitted(offsets []TopicPartition) {
	cb.mu.RLock()
	if cb.OnOffsetsCommitted != nil {
		cb.OnOffsetsCommitted(offsets)
	}
	cb.mu.RUnlock()
}

// ConsumerCallback will receive events from producers
type ConsumerCallback interface {
	// OnDataReceived is called when an event is received
	DataReceived(msg Message) error
	// OnErrorReceived is called when an error is received
	ErrorReceived(err error)
}

// TopicPartition has information about the partition
type TopicPartition struct {
	Topic     string
	Partition int32
	Offset    int64
}

// ConsumerCallbackPartitionLifecycle will receive events for partition lifecycle changes
type ConsumerCallbackPartitionLifecycle interface {
	// OnPartitionAssignment is called when partitions are assigned to the consumer
	PartitionAssignment(partitions []TopicPartition)
	// OnPartitionRevocation is called when partitions are unassigned to the consumer
	PartitionRevocation(partitions []TopicPartition)
	// OnOffsetsCommitted is called when offsets are committed
	OffsetsCommitted(offsets []TopicPartition)
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
	// Pause will allow the consumer to be stopped temporarily from processing further messages
	Pause() error
	// Resume will allow the paused consumer to be resumed
	Resume() error
}
