package kafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/pinpt/go-common/eventing"
	ck "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// ErrMissingTopic is an error that is returned if the topic is missing in the Message
var ErrMissingTopic = errors.New("error: missing topic in message")

// Producer will emit events to kafka
type Producer struct {
	config   Config
	producer *ck.Producer
	closed   bool
	mu       sync.RWMutex
	size     int64
	count    int64
}

var _ eventing.Producer = (*Producer)(nil)

// Count returns the number of records transmitted
func (p *Producer) Count() int64 {
	p.mu.RLock()
	val := p.count
	p.mu.RUnlock()
	return val
}

// Count returns the number of bytes transmitted
func (p *Producer) Size() int64 {
	p.mu.RLock()
	val := p.size
	p.mu.RUnlock()
	return val
}

// Send will send the event
func (p *Producer) Send(ctx context.Context, msg eventing.Message) error {
	if msg.Topic == "" {
		return ErrMissingTopic
	}
	headers := make([]ck.Header, 0)
	if msg.Headers != nil {
		for k, v := range msg.Headers {
			headers = append(headers, ck.Header{
				Key:   k,
				Value: []byte(v),
			})
		}
	}
	headers = append(headers, ck.Header{
		Key:   "encoding",
		Value: []byte(msg.Encoding),
	})
	tp := ck.TopicPartition{
		Topic:     &msg.Topic,
		Partition: msg.Partition,
	}
	timestamp := msg.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	value := msg.Value
	p.mu.Lock()
	closed := p.closed
	if !closed {
		p.size += int64(len(value))
		p.count++
	}
	p.mu.Unlock()
	if !closed {
		// make a copy since this is going to be held internally by the producer channel queue
		val := make([]byte, len(value))
		copy(val, value)
		p.producer.ProduceChannel() <- &ck.Message{
			TopicPartition: tp,
			Key:            []byte(msg.Key),
			Value:          val,
			Timestamp:      timestamp,
			Headers:        headers,
		}
	}
	return nil
}

// Close will close the producer
func (p *Producer) Close() error {
	p.mu.Lock()
	closed := p.closed
	p.closed = true
	p.mu.Unlock()
	if !closed {
		p.producer.Flush(int((5 * time.Second) / time.Millisecond))
		p.producer.Close()
	}
	return nil
}

// NewProducer returns a new Producer instance
func NewProducer(config Config) (*Producer, error) {
	c := NewConfigMap(config)
	// See below link for other configuration options
	// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	if err := c.SetKey("compression.codec", "snappy"); err != nil {
		return nil, err
	}
	if err := c.SetKey("go.delivery.reports", false); err != nil {
		return nil, err
	}
	// defaults to 100000
	if err := c.SetKey("queue.buffering.max.messages", 500000); err != nil {
		return nil, err
	}
	// defaults to 0.5
	if err := c.SetKey("queue.buffering.max.ms", 1); err != nil {
		return nil, err
	}
	producer, err := ck.NewProducer(c)
	if err != nil {
		return nil, err
	}
	return &Producer{
		config:   config,
		producer: producer,
	}, nil
}
