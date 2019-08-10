package kafka

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/pinpt/go-common/eventing"
	"github.com/stretchr/testify/assert"
)

func TestCreateTopicFromProducer(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	config := Config{
		Brokers: []string{"localhost:9092"},
	}
	producer, err := NewProducer(config)
	assert.NoError(err)
	defer producer.Close()
	client, err := NewAdminClientUsingProducer(producer)
	assert.NoError(err)
	assert.NoError(client.NewTopic("testtopic", TopicConfig{}))
}

func TestCreateTopicFromConsumer(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	config := Config{
		Brokers: []string{"localhost:9092"},
	}
	consumer, err := NewConsumer(config, "testgroup", "testtopic")
	assert.NoError(err)
	defer consumer.Close()
	client, err := NewAdminClientUsingConsumer(consumer)
	assert.NoError(err)
	assert.NoError(client.NewTopic("testtopic", TopicConfig{}))
}

func TestSendReceiveCallback(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	config := Config{
		Brokers: []string{"localhost:9092"},
	}
	producer, err := NewProducer(config)
	assert.NoError(err)
	defer producer.Close()
	consumer, err := NewConsumer(config, "testgroup", "testtopic")
	assert.NoError(err)
	defer consumer.Close()
	done := make(chan bool, 1)
	consumer.Consume(&eventing.ConsumerCallbackAdapter{
		OnDataReceived: func(msg eventing.Message) error {
			assert.Equal("foo", msg.Key)
			assert.True(bytes.Equal([]byte("value"), msg.Value))
			assert.Equal("bar", msg.Headers["foo"])
			assert.Equal("testtopic", msg.Topic)
			assert.False(msg.Timestamp.IsZero())
			assert.True(msg.IsAutoCommit())
			done <- true
			return nil
		},
	})
	assert.NoError(producer.Send(context.Background(), eventing.Message{
		Key:   "foo",
		Value: []byte("value"),
		Topic: "testtopic",
		Headers: map[string]string{
			"foo": "bar",
		},
	}))
	<-done
}

func TestSendReceiveCallbackWithAutoCommit(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	config := Config{
		Brokers:           []string{"localhost:9092"},
		DisableAutoCommit: true,
	}
	producer, err := NewProducer(config)
	assert.NoError(err)
	defer producer.Close()
	consumer, err := NewConsumer(config, "testgroup", "testtopic")
	assert.NoError(err)
	defer consumer.Close()
	admin, err := NewAdminClientUsingConsumer(consumer)
	assert.NoError(err)
	assert.NoError(admin.DeleteTopic("testtopic"))
	assert.NoError(admin.NewTopic("testtopic", TopicConfig{NumPartitions: 1, ReplicationFactor: 1}))
	done := make(chan bool, 1)
	consumer.Consume(&eventing.ConsumerCallbackAdapter{
		OnDataReceived: func(msg eventing.Message) error {
			assert.Equal("foo", msg.Key)
			assert.True(bytes.Equal([]byte("value"), msg.Value))
			assert.Equal("bar", msg.Headers["foo"])
			assert.Equal("testtopic", msg.Topic)
			assert.False(msg.Timestamp.IsZero())
			assert.False(msg.IsAutoCommit())
			assert.NoError(msg.Commit())
			done <- true
			return nil
		},
	})
	assert.NoError(producer.Send(context.Background(), eventing.Message{
		Key:   "foo",
		Value: []byte("value"),
		Topic: "testtopic",
		Headers: map[string]string{
			"foo": "bar",
		},
	}))
	<-done
}

func TestSendReceiveCallbackWithResetOffset(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	config := Config{
		Brokers:           []string{"localhost:9092"},
		DisableAutoCommit: true,
	}
	producer, err := NewProducer(config)
	assert.NoError(err)
	defer producer.Close()
	consumer, err := NewConsumer(config, "testgroup", "testtopic")
	assert.NoError(err)
	assert.NotNil(consumer)
	defer consumer.Close()
	admin, err := NewAdminClientUsingConsumer(consumer)
	assert.NoError(err)
	assert.NoError(admin.DeleteTopic("testtopic"))
	assert.NoError(admin.NewTopic("testtopic", TopicConfig{NumPartitions: 1, ReplicationFactor: 1}))
	done := make(chan bool)
	consumer.Consume(&eventing.ConsumerCallbackAdapter{
		OnDataReceived: func(msg eventing.Message) error {
			assert.Equal("foo", msg.Key)
			assert.True(bytes.Equal([]byte("value"), msg.Value))
			assert.Equal("bar", msg.Headers["foo"])
			assert.Equal("testtopic", msg.Topic)
			assert.Equal(int64(0), msg.Offset)
			assert.False(msg.Timestamp.IsZero())
			assert.False(msg.IsAutoCommit())
			assert.NoError(msg.Commit())
			done <- true
			return nil
		},
	})
	assert.NoError(producer.Send(context.Background(), eventing.Message{
		Key:   "foo",
		Value: []byte("value"),
		Topic: "testtopic",
		Headers: map[string]string{
			"foo": "bar",
		},
	}))
	<-done
	consumer.Close()
	producer.Close()
	done2 := make(chan bool)
	config.ResetOffset = true // reset to acquire from the beginning
	consumer2, err := NewConsumer(config, "testgroup", "testtopic")
	assert.NoError(err)
	defer consumer2.Close()
	consumer2.Consume(&eventing.ConsumerCallbackAdapter{
		OnDataReceived: func(msg eventing.Message) error {
			assert.Equal("foo", msg.Key)
			assert.True(bytes.Equal([]byte("value"), msg.Value))
			assert.Equal("bar", msg.Headers["foo"])
			assert.Equal("testtopic", msg.Topic)
			assert.Equal(int64(0), msg.Offset)
			assert.False(msg.Timestamp.IsZero())
			assert.False(msg.IsAutoCommit())
			assert.NoError(msg.Commit())
			done2 <- true
			return nil
		},
	})
	<-done2
}

type eofcallback struct {
	eventing.ConsumerCallbackAdapter
	eof func(topic string, partition int32, offset int64)
}

func (c *eofcallback) EOF(topic string, partition int32, offset int64) {
	c.eof(topic, partition, offset)
}

func TestSendReceiveCallbackEOF(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	config := Config{
		Brokers: []string{"localhost:9092"},
	}
	producer, err := NewProducer(config)
	assert.NoError(err)
	defer producer.Close()
	consumer, err := NewConsumer(config, "testgroup2", "testtopic")
	assert.NoError(err)
	defer consumer.Close()
	done := make(chan bool, 1)
	callback := &eofcallback{
		eventing.ConsumerCallbackAdapter{
			OnDataReceived: func(msg eventing.Message) error {
				assert.Equal("foo", msg.Key)
				assert.True(bytes.Equal([]byte("value"), msg.Value))
				assert.Equal("bar", msg.Headers["foo"])
				assert.Equal("testtopic", msg.Topic)
				assert.False(msg.Timestamp.IsZero())
				return nil
			},
		},
		func(topic string, partition int32, offset int64) {
			assert.Equal("testtopic", topic)
			assert.True(offset > 0)
			done <- true
		},
	}
	consumer.Consume(callback)
	assert.NoError(producer.Send(context.Background(), eventing.Message{
		Key:   "foo",
		Value: []byte("value"),
		Topic: "testtopic",
		Headers: map[string]string{
			"foo": "bar",
		},
	}))
	<-done
}

func TestSendReceiveCallbackStats(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	config := Config{
		Brokers: []string{"localhost:9092"},
	}
	producer, err := NewProducer(config)
	assert.NoError(err)
	defer producer.Close()
	config.Extra = map[string]interface{}{
		"statistics.interval.ms": 5000,
	}
	consumer, err := NewConsumer(config, "testgroup3", "testtopic")
	assert.NoError(err)
	defer consumer.Close()
	done := make(chan bool, 1)
	callback := &eventing.ConsumerCallbackAdapter{
		OnStats: func(stats map[string]interface{}) {
			assert.NotEmpty(stats)
			done <- true
		},
	}
	consumer.Consume(callback)
	assert.NoError(producer.Send(context.Background(), eventing.Message{
		Key:   "foo",
		Value: []byte("value"),
		Topic: "testtopic",
		Headers: map[string]string{
			"foo": "bar",
		},
	}))
	<-done
}

func TestConsumerPing(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	config := Config{
		Brokers: []string{"localhost:9092"},
	}
	consumer, err := NewPingConsumer(config)
	assert.NoError(err)
	defer consumer.Close()
	assert.True(consumer.Ping())
}

func TestPing(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	config := RegistryConfig{
		URL: "http://localhost:8081",
	}
	c := NewRegistryClient(config)
	assert.True(c.Ping())
}
