package kafka

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pinpt/go-common/datetime"
	"github.com/pinpt/go-common/eventing"
	"github.com/pinpt/go-common/log"
	ck "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	// CompressionHeader is the name of the header used to indicate compressed value
	CompressionHeader = "pinpt-compression"
	// CompressionGzip is the value to indicate the compression type
	CompressionGzip = "gzip"
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
	ch       chan ck.Event
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
	if timestamp.IsZero() || datetime.TimeToEpoch(timestamp) == 0 {
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
		var val []byte
		if p.config.Gzip {
			var buf bytes.Buffer
			gz, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
			if err != nil {
				return fmt.Errorf("error creating gzip writer: %w", err)
			}
			gz.Write(value)
			gz.Close()
			// do the compression and indicate that we have compressed data
			val = buf.Bytes()
			headers = append(headers, ck.Header{
				Key:   CompressionHeader,
				Value: []byte(CompressionGzip),
			})
		} else {
			// make a copy since this is going to be held internally by the producer channel queue
			val = make([]byte, len(value))
			copy(val, value)
		}
		msg := &ck.Message{
			TopicPartition: tp,
			Key:            []byte(msg.Key),
			Value:          val,
			Timestamp:      timestamp,
			Headers:        headers,
		}
		if err := p.producer.Produce(msg, p.ch); err != nil {
			return err
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
		close(p.ch)
	}
	return nil
}

func (p *Producer) run() {
	for evt := range p.ch {
		if m, ok := evt.(*ck.Message); ok {
			if m.TopicPartition.Error != nil {
				if p.config.Logger != nil {
					log.Error(p.config.Logger, "error sending kafka message", "topic", *m.TopicPartition.Topic, "err", m.TopicPartition.Error, "id", string(m.Key), "partition", m.TopicPartition.Partition)
				} else {
					fmt.Fprintf(os.Stderr, "error sending kafka message to %v. %v\n", *m.TopicPartition.Topic, m.TopicPartition.Error)
				}
			}
		}
	}
}

// NewProducer returns a new Producer instance
func NewProducer(config Config) (*Producer, error) {
	c := NewConfigMap(config)
	// See below link for other configuration options
	// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	if !config.Gzip {
		if err := c.SetKey("compression.codec", "snappy"); err != nil {
			return nil, err
		}
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
	p := &Producer{
		config:   config,
		producer: producer,
		ch:       make(chan ck.Event, 10),
	}
	go p.run()
	return p, nil
}
