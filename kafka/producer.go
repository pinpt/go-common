package kafka

import (
	"context"
	"encoding/binary"
	"errors"
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
}

var _ eventing.Producer = (*Producer)(nil)

// Send will send the event
func (p *Producer) Send(ctx context.Context, msg eventing.Message) error {
	if msg.Topic == "" {
		return ErrMissingTopic
	}
	var headers []ck.Header
	if msg.Headers != nil {
		headers = make([]ck.Header, 0)
		for k, v := range msg.Headers {
			headers = append(headers, ck.Header{
				Key:   k,
				Value: []byte(v),
			})
		}
	}
	tp := ck.TopicPartition{
		Topic:     &msg.Topic,
		Partition: msg.Partition,
	}
	timestamp := msg.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	value := msg.Value
	if p.config.Registry != nil && msg.Codec != nil && msg.Encoding == eventing.AvroEncoding {
		subject := msg.Topic + "-value"
		schemaid, err := p.config.Registry.CreateSubject(subject, msg.Codec)
		if err != nil {
			return err
		}
		// encode the avro buffer
		binarySchemaId := make([]byte, 4)
		binary.BigEndian.PutUint32(binarySchemaId, uint32(schemaid))

		switch msg.Encoding {
		case eventing.AvroEncoding:
			break // already in the right format
		default:
			native, _, err := msg.Codec.NativeFromTextual(value)
			if err != nil {
				return err
			}
			// Convert native Go form to binary Avro data
			binaryValue, err := msg.Codec.BinaryFromNative(nil, native)
			if err != nil {
				return err
			}
			value = binaryValue
		}

		var binaryMsg []byte
		// first byte is magic byte, always 0 for now
		binaryMsg = append(binaryMsg, byte(0))
		//4-byte schema ID as returned by the Schema Registry
		binaryMsg = append(binaryMsg, binarySchemaId...)
		//avro serialized data in Avro’s binary encoding
		binaryMsg = append(binaryMsg, value...)
		// reset the value to the new binary encoded value
		value = binaryMsg
	}
	return p.producer.Produce(&ck.Message{
		TopicPartition: tp,
		Key:            []byte(msg.Key),
		Value:          value,
		Timestamp:      timestamp,
		Headers:        headers,
	}, nil)
}

// Close will close the producer
func (p *Producer) Close() error {
	p.producer.Close()
	return nil
}

// NewProducer returns a new Producer instance
func NewProducer(config Config) (*Producer, error) {
	c := NewConfigMap(config)
	c.SetKey("compression.codec", "snappy")
	producer, err := ck.NewProducer(c)
	if err != nil {
		return nil, err
	}
	return &Producer{
		config:   config,
		producer: producer,
	}, nil
}
