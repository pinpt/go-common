package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pinpt/go-common/eventing"
	ck "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// ErrMissingTopics is returned if no topics are passed
var ErrMissingTopics = errors.New("error: missing at least one topic for consumer")

// ConsumerEOFCallback is an interface for handling topic EOF events
type ConsumerEOFCallback interface {
	EOF(topic string, partition int32, offset int64)
}

// ConsumerStatsCallback is an interface for handling stats events
type ConsumerStatsCallback interface {
	Stats(stats map[string]interface{})
}

// Consumer will return a kafka consumer
type Consumer struct {
	config          Config
	consumer        *ck.Consumer
	done            chan struct{}
	DefaultPollTime time.Duration
	mu              sync.Mutex
	closed          bool
}

var _ eventing.Consumer = (*Consumer)(nil)

// Close will stop listening for events
func (c *Consumer) Close() error {
	c.mu.Lock()
	closed := c.closed
	c.closed = true
	c.mu.Unlock()
	var err error
	if !closed {
		c.consumer.Unsubscribe()
		c.done <- struct{}{}
		err = c.consumer.Close()
	}
	return err
}

// Ping will cause a ping against the broker by way of fetching metadata from the _schemas topic
func (c *Consumer) Ping() bool {
	topic := "_schemas"
	md, err := c.consumer.GetMetadata(&topic, false, 2000)
	return err == nil && len(md.Topics) == 1
}

// Consume will start consuming from the consumer using the callback
func (c *Consumer) Consume(callback eventing.ConsumerCallback) {
	go func() {
		for {
			select {
			case <-c.done:
				return
			default:
				c.mu.Lock()
				closed := c.closed
				c.mu.Unlock()
				if closed {
					return
				}
				c.mu.Lock()
				ev := c.consumer.Poll(int(c.DefaultPollTime / time.Millisecond))
				c.mu.Unlock()
				// fmt.Println(ev, reflect.TypeOf(ev))
				if ev == nil {
					continue
				}
				defer func() {
					// don't allow a panic
					if x := recover(); x != nil {
						fmt.Fprintf(os.Stderr, "panic: %v\n", x)
					}
				}()
				// fmt.Println("EVENT", ev, "=>", reflect.ValueOf(ev))
				switch e := ev.(type) {
				case ck.AssignedPartitions:
					c.consumer.Assign(e.Partitions)
				case ck.RevokedPartitions:
					c.consumer.Unassign()
				case ck.Error:
					// Generic client instance-level errors, such as
					// broker connection failures, authentication issues, etc.
					//
					// These errors should generally be considered informational
					// as the underlying client will automatically try to
					// recover from any errors encountered, the application
					// does not need to take action on them.
					//
					// But with idempotence enabled, truly fatal errors can
					// be raised when the idempotence guarantees can't be
					// satisfied, these errors are identified by
					// `e.IsFatal()`.
					if e.IsFatal() {
						callback.ErrorReceived(e)
						return
					}
				case *ck.Message:
					headers := make(map[string]string)
					if e.Headers != nil {
						for _, h := range e.Headers {
							headers[h.Key] = string(h.Value)
						}
					}
					var topic string
					if e.TopicPartition.Topic != nil {
						topic = *e.TopicPartition.Topic
					}
					buf := make([]byte, len(e.Value))
					copy(buf, e.Value)
					if err := callback.DataReceived(eventing.Message{
						Encoding:  eventing.AvroEncoding,
						Key:       string(e.Key),
						Value:     buf,
						Headers:   headers,
						Timestamp: e.Timestamp,
						Topic:     topic,
						Partition: e.TopicPartition.Partition,
					}); err != nil {
						callback.ErrorReceived(err)
					}
				case ck.PartitionEOF:
					if cb, ok := callback.(ConsumerEOFCallback); ok {
						cb.EOF(*e.Topic, e.Partition, int64(e.Offset))
					}
				case *ck.Stats:
					// Stats events are emitted as JSON (as string).
					// Either directly forward the JSON to your
					// statistics collector, or convert it to a
					// map to extract fields of interest.
					// The definition of the statistics JSON
					// object can be found here:
					// https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
					var stats map[string]interface{}
					if err := json.Unmarshal([]byte(e.String()), &stats); err == nil {
						if cb, ok := callback.(ConsumerStatsCallback); ok {
							cb.Stats(stats)
						}
					}
				}
			}
		}
	}()
}

// NewConsumer returns a new Consumer instance
func NewConsumer(config Config, groupid string, topics ...string) (*Consumer, error) {
	if len(topics) == 0 {
		return nil, ErrMissingTopics
	}
	cfg := NewConfigMap(config)
	cfg.SetKey("group.id", groupid)
	cfg.SetKey("enable.partition.eof", true)
	cfg.SetKey("go.events.channel.enable", false)
	cfg.SetKey("go.application.rebalance.enable", true)
	if config.Offset == "" {
		cfg.SetKey("auto.offset.reset", ck.OffsetBeginning)
	} else {
		cfg.SetKey("auto.offset.reset", config.Offset)
	}
	consumer, err := ck.NewConsumer(cfg)
	if err != nil {
		return nil, err
	}
	if err := consumer.SubscribeTopics(topics, nil); err != nil {
		return nil, err
	}
	c := &Consumer{
		config:          config,
		consumer:        consumer,
		done:            make(chan struct{}, 1),
		DefaultPollTime: time.Millisecond * 500,
	}
	return c, nil
}

// NewPingConsumer returns a new Consumer instance that supports only pings
func NewPingConsumer(config Config) (*Consumer, error) {
	cfg := NewConfigMap(config)
	cfg.SetKey("group.id", "kafka.ping.consumer")
	cfg.SetKey("go.events.channel.enable", false)
	consumer, err := ck.NewConsumer(cfg)
	if err != nil {
		return nil, err
	}
	c := &Consumer{
		config:   config,
		consumer: consumer,
		done:     make(chan struct{}, 1),
	}
	return c, nil
}
