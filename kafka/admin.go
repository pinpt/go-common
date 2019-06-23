package kafka

import (
	"context"
	"fmt"
	"time"

	ck "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type TopicConfig struct {
	NumPartitions     int
	ReplicationFactor int
	RetentionPeriod   time.Duration
	MaxMessageSize    int64
	Config            map[string]string
}

// AdminClient provides an interfae for talking with the Kafka admin
type AdminClient interface {
	// NewTopic will create a new topic
	NewTopic(name string, config TopicConfig) error
}

type AdminClientImpl struct {
	client *ck.AdminClient
}

var _ AdminClient = (*AdminClientImpl)(nil)

func (c *AdminClientImpl) NewTopic(name string, config TopicConfig) error {
	cfg := config.Config
	if cfg == nil {
		cfg = make(map[string]string)
	}
	partitions := config.NumPartitions
	if partitions == 0 {
		partitions = 1
	}
	replicationFactor := config.ReplicationFactor
	if replicationFactor == 0 {
		replicationFactor = 1
	}
	if config.MaxMessageSize > 0 {
		cfg["max.message.bytes"] = fmt.Sprintf("%d", config.MaxMessageSize)
	}
	if config.RetentionPeriod > 0 {
		cfg["retention.ms"] = fmt.Sprintf("%d", int64(config.RetentionPeriod/time.Millisecond))
	}
	_, err := c.client.CreateTopics(context.Background(), []ck.TopicSpecification{
		ck.TopicSpecification{
			Topic:             name,
			NumPartitions:     partitions,
			ReplicationFactor: replicationFactor,
			Config:            cfg,
		},
	})
	return err
}

// NewAdminClientUsingProducer will create a new AdminClient from a Producer
func NewAdminClientUsingProducer(p *Producer) (*AdminClientImpl, error) {
	client, err := ck.NewAdminClientFromProducer(p.producer)
	if err != nil {
		return nil, err
	}
	return &AdminClientImpl{client}, nil
}

// NewAdminClientUsingConsumer will create a new AdminClient from a Consumer
func NewAdminClientUsingConsumer(c *Consumer) (*AdminClientImpl, error) {
	client, err := ck.NewAdminClientFromConsumer(c.consumer)
	if err != nil {
		return nil, err
	}
	return &AdminClientImpl{client}, nil
}
