package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
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
	autocommit      bool
	shouldreset     bool
	hasreset        bool
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

// Pause will allow the consumer to be stopped temporarily from processing further messages
func (c *Consumer) Pause() error {
	assignments, err := c.consumer.Assignment()
	if err != nil {
		return fmt.Errorf("error fetching assignment for pausing. %v", err)
	}
	return c.consumer.Pause(assignments)
}

// Resume will allow the paused consumer to be resumed
func (c *Consumer) Resume() error {
	assignments, err := c.consumer.Assignment()
	if err != nil {
		return fmt.Errorf("error fetching assignment for resuming. %v", err)
	}
	return c.consumer.Resume(assignments)
}

// Ping will cause a ping against the broker by way of fetching metadata from the _schemas topic
func (c *Consumer) Ping() bool {
	topic := "_schemas"
	md, err := c.consumer.GetMetadata(&topic, false, 2000)
	return err == nil && len(md.Topics) == 1
}

func toEventingPartitions(topicpartitions []ck.TopicPartition) []eventing.TopicPartition {
	tp := make([]eventing.TopicPartition, 0)
	for _, partition := range topicpartitions {
		tp = append(tp, eventing.TopicPartition{
			Partition: partition.Partition,
			Offset:    int64(partition.Offset),
		})
	}
	return tp
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
				if ev == nil {
					continue
				}
				defer func() {
					// don't allow a panic
					if x := recover(); x != nil {
						fmt.Fprintf(os.Stderr, "panic: %v\n", x)
					}
				}()
				// fmt.Println("EVENT", ev, "=>", reflect.ValueOf(ev), "reset", c.shouldreset, "hasreset", c.hasreset)
				switch e := ev.(type) {
				case ck.AssignedPartitions:
					if c.shouldreset && !c.hasreset {
						// if we are resetting our offset, we want to do that after we
						// receive the new assignments ... so we can instruct the
						// consumer to beginning from here instaed of the stored location
						newoffset, _ := ck.NewOffset(int64(0))
						// we have to make a copy since the incoming isn't a pointer struct
						topicpartitions := make([]ck.TopicPartition, 0)
						for _, partition := range e.Partitions {
							topicpartitions = append(topicpartitions, ck.TopicPartition{
								Topic:     partition.Topic,
								Partition: partition.Partition,
								Offset:    newoffset,
							})
						}
						c.hasreset = true
						if err := c.consumer.Assign(topicpartitions); err != nil {
							callback.ErrorReceived(err)
							return
						}
						if ci, ok := callback.(eventing.ConsumerCallbackPartitionLifecycle); ok {
							ci.PartitionAssignment(toEventingPartitions(topicpartitions))
						}
						continue
					}
					if err := c.consumer.Assign(e.Partitions); err != nil {
						callback.ErrorReceived(err)
						return
					}
					if ci, ok := callback.(eventing.ConsumerCallbackPartitionLifecycle); ok {
						ci.PartitionAssignment(toEventingPartitions(e.Partitions))
					}
				case ck.RevokedPartitions:
					if err := c.consumer.Unassign(); err != nil {
						callback.ErrorReceived(err)
						return
					}
					if ci, ok := callback.(eventing.ConsumerCallbackPartitionLifecycle); ok {
						ci.PartitionRevocation(toEventingPartitions(e.Partitions))
					}
				case ck.OffsetsCommitted:
					if ci, ok := callback.(eventing.ConsumerCallbackPartitionLifecycle); ok {
						ci.OffsetsCommitted(toEventingPartitions(e.Offsets))
					}
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
					// check to see if the consumer implements the callback filter interface
					// and if so, let it determine
					if ci, ok := callback.(eventing.ConsumerCallbackMessageFilter); ok {
						if !ci.ShouldProcess(e) {
							if !c.autocommit {
								c.consumer.CommitMessage(e)
							}
							// if we have rejected it, we should return
							continue
						}
					}
					// check to see if the consumer wants to decide on handling the kafka
					// message before handing it off for deserialization
					if c.config.ShouldProcessKafkaMessage != nil {
						if !c.config.ShouldProcessKafkaMessage(e) {
							if !c.autocommit {
								c.consumer.CommitMessage(e)
							}
							// if we have rejected it, we should return
							continue
						}
					}
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
					var encoding eventing.ValueEncodingType
					if buf[0] == '{' && buf[len(buf)-1] == '}' {
						encoding = eventing.JSONEncoding
					} else {
						encoding = eventing.AvroEncoding
					}
					offset, err := strconv.ParseInt(e.TopicPartition.Offset.String(), 10, 64)
					if err != nil {
						fmt.Printf("error parsing the offset (%v): %v\n", e.TopicPartition.Offset.String(), err)
					}
					msg := eventing.Message{
						Encoding:  encoding,
						Key:       string(e.Key),
						Value:     buf,
						Headers:   headers,
						Timestamp: e.Timestamp,
						Topic:     topic,
						Partition: e.TopicPartition.Partition,
						Offset:    offset,
						CommitOverride: func(m eventing.Message) error {
							_, err := c.consumer.CommitMessage(e)
							return err
						},
						AutoCommit: c.autocommit,
					}
					// check to see if the consumer implements the callback filter interface
					// and if so, let it determine
					if ci, ok := callback.(eventing.ConsumerCallbackEventFilter); ok {
						if !ci.ShouldFilter(&msg) {
							if !c.autocommit {
								c.consumer.CommitMessage(e)
							}
							// if we have rejected it, we should return
							continue
						}
					}
					// check to see if the config wants to filter the messages
					if c.config.ShouldProcessEventMessage != nil {
						if !c.config.ShouldProcessEventMessage(&msg) {
							if !c.autocommit {
								c.consumer.CommitMessage(e)
							}
							// if we have rejected it, we should return
							continue
						}
					}
					if err := callback.DataReceived(msg); err != nil {
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
					if cb, ok := callback.(ConsumerStatsCallback); ok {
						var stats map[string]interface{}
						if err := json.Unmarshal([]byte(e.String()), &stats); err == nil {
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
	if err := cfg.SetKey("group.id", groupid); err != nil {
		return nil, err
	}
	if err := cfg.SetKey("enable.partition.eof", true); err != nil {
		return nil, err
	}
	if err := cfg.SetKey("go.events.channel.enable", false); err != nil {
		return nil, err
	}
	if err := cfg.SetKey("go.application.rebalance.enable", true); err != nil {
		return nil, err
	}
	if config.Offset == "" {
		if err := cfg.SetKey("auto.offset.reset", ck.OffsetBeginning); err != nil {
			return nil, err
		}
	} else {
		if err := cfg.SetKey("auto.offset.reset", config.Offset); err != nil {
			return nil, err
		}
	}
	val, _ := cfg.Get("auto.offset.reset", nil)
	if val != nil {
		switch t := val.(type) {
		case ck.Offset:
			break
		case string:
			switch t {
			case "earliest", "latest", "none", "smallest", "largest", "beginning":
				break
			default:
				return nil, fmt.Errorf("error for kafka consumer setting 'auto.offset.reset'. must be one of: earliest, latest, none, smallest, largest or beginning. was: %v", val)
			}
		}
	}
	consumer, err := ck.NewConsumer(cfg)
	if err != nil {
		return nil, err
	}
	c := &Consumer{
		config:          config,
		consumer:        consumer,
		done:            make(chan struct{}, 1),
		DefaultPollTime: time.Millisecond * 500,
		autocommit:      !config.DisableAutoCommit,
		shouldreset:     config.ResetOffset,
	}
	if err := consumer.SubscribeTopics(topics, nil); err != nil {
		return nil, err
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
