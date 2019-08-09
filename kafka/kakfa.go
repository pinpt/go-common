package kafka

import (
	"strings"

	ck "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Config holds the configuration for connection to the broker
type Config struct {
	Brokers           []string
	Username          string
	Password          string
	Extra             map[string]interface{}
	Registry          RegistryClient
	Offset            string
	DisableAutoCommit bool
}

// NewConfigMap returns a ConfigMap from a Config
func NewConfigMap(config Config) *ck.ConfigMap {
	c := &ck.ConfigMap{
		"request.timeout.ms":  20000,
		"retry.backoff.ms":    500,
		"api.version.request": true,
		"message.max.bytes":   1000000000, // allow the consumer/producer to be as big but the broker to enforce
		"bootstrap.servers":   strings.Join(config.Brokers, ","),
		"client.id":           "pinpt/go-common",
	}
	if config.DisableAutoCommit {
		c.SetKey("enable.auto.commit", "false")
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
	return c
}
