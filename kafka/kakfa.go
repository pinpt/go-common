package kafka

import (
	"context"
	"strings"
	"time"

	"github.com/pinpt/go-common/eventing"
	"github.com/pinpt/go-common/log"
	ck "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// ShouldProcessKafkaMessage is a handler for deciding if we should process the incoming kafka message before it's deserialized
type ShouldProcessKafkaMessage func(msg *ck.Message) bool

// ShouldProcessEventMessage is a handler for deciding if we should process the event after deserialization but before we deliver to consumer handler
type ShouldProcessEventMessage func(msg *eventing.Message) bool

// DefaultMinGzipBytes is the minimum size of data before we compress (assuming config.Gzip = true)
const DefaultMinGzipBytes = 1024

// Config holds the configuration for connection to the broker
type Config struct {
	Brokers                   []string
	Username                  string
	Password                  string
	Extra                     map[string]interface{}
	Offset                    string
	DisableAutoCommit         bool
	ResetOffset               bool
	ShouldProcessKafkaMessage ShouldProcessKafkaMessage
	ShouldProcessEventMessage ShouldProcessEventMessage
	ClientID                  string
	DefaultPollTime           time.Duration // only for consumers
	Context                   context.Context
	Logger                    log.Logger
	Gzip                      bool
	GzipMinBytes              int  // if not set, defaults to DefaultMinGzipBytes
	IgnoreAssignedOffsets     bool // manage your own assignments
	ProcessDuration           time.Duration
}

// NewConfigMap returns a ConfigMap from a Config
func NewConfigMap(config Config) *ck.ConfigMap {
	clientid := config.ClientID
	if clientid == "" {
		clientid = "pinpt/go-common"
	}
	c := &ck.ConfigMap{
		"request.timeout.ms":       20000,
		"retry.backoff.ms":         500,
		"api.version.request":      true,
		"message.max.bytes":        1000000000, // allow the consumer/producer to be as big but the broker to enforce
		"bootstrap.servers":        strings.Join(config.Brokers, ","),
		"client.id":                clientid,
		"enable.auto.offset.store": true,
		"session.timeout.ms":       10000,
		"heartbeat.interval.ms":    3000,
		"log_level":                2,
	}
	if config.DisableAutoCommit {
		c.SetKey("enable.auto.commit", "false")
		c.SetKey("enable.auto.offset.store", false)
	}
	if config.Username != "" {
		c.SetKey("sasl.mechanism", "PLAIN")
		c.SetKey("security.protocol", "SASL_SSL")
		c.SetKey("sasl.username", config.Username)
		c.SetKey("sasl.password", config.Password)
	}
	if config.Extra != nil {
		for k, v := range config.Extra {
			c.SetKey(k, v)
		}
	}
	if config.Context == nil {
		config.Context = context.Background()
	}
	return c
}
