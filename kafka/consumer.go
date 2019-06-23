package kafka

import (
	"time"

	"github.com/pinpt/go-common/eventing"
	ck "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// ConsumerEOFCallback is an interface for handling topic EOF events
type ConsumerEOFCallback interface {
	EOF(topic string, partition int32, offset int64)
}

// Consumer will return a kafka consumer
type Consumer struct {
	config          Config
	consumer        *ck.Consumer
	done            chan struct{}
	DefaultPollTime time.Duration
}

var _ eventing.Consumer = (*Consumer)(nil)

// Close will stop listening for events
func (c *Consumer) Close() error {
	c.done <- struct{}{}
	return c.consumer.Close()
}

// Consume will start consuming from the consumer using the callback
func (c *Consumer) Consume(callback eventing.ConsumerCallback) {
	go func() {
		for {
			select {
			case <-c.done:
				return
			default:
				ev := c.consumer.Poll(int(c.DefaultPollTime / time.Millisecond))
				// fmt.Println(ev, reflect.TypeOf(ev))
				if ev == nil {
					continue
				}
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
					callback.DataReceived(eventing.Message{
						Key:       string(e.Key),
						Value:     e.Value,
						Headers:   headers,
						Timestamp: e.Timestamp,
						Topic:     topic,
						Partition: e.TopicPartition.Partition,
					})
				case ck.PartitionEOF:
					if cb, ok := callback.(ConsumerEOFCallback); ok {
						cb.EOF(*e.Topic, e.Partition, int64(e.Offset))
					}
				}
			}
		}
	}()
}

// NewConsumer returns a new Consumer instance
func NewConsumer(config Config, groupid string, topics ...string) (*Consumer, error) {
	cfg := NewConfigMap(config)
	cfg.SetKey("group.id", groupid)
	cfg.SetKey("enable.partition.eof", true)
	cfg.SetKey("go.events.channel.enable", false)
	cfg.SetKey("go.application.rebalance.enable", true)
	cfg.SetKey("auto.offset.reset", ck.OffsetBeginning)
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
		DefaultPollTime: time.Second * 5,
	}
	return c, nil
}
