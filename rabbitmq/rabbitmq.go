package rabbitmq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/pinpt/go-common/v10/log"
	pstrings "github.com/pinpt/go-common/v10/strings"
	"github.com/streadway/amqp"
)

// Config for the session
type Config struct {
	Name                    string
	ID                      string
	Exchange                string
	ConsumerConnectionPool  *ConnectionPool
	PublisherConnectionPool *ConnectionPool
	AutoAck                 bool
	DurableQueue            bool
	DeleteUnused            bool
	Exclusive               bool
	Args                    amqp.Table
	Qos                     int
	PublishOnly             bool
	RoutingKeys             []string
	Context                 context.Context
}

// Session is the rabbitmq session
type Session struct {
	messages             chan amqp.Delivery
	logger               log.Logger
	config               *Config
	name                 string
	consumerchannelhost  *ChannelHost
	publisherchannelhost *ChannelHost
	done                 chan bool
	mu                   sync.RWMutex
	bindings             []string
	autocommit           bool
	inflightmessages     map[uint64]bool
}

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 2 * time.Second

	// When setting up the channel after a channel exception
	reInitDelay = 10 * time.Millisecond

	// When sending how long to wait for confirm before bailing
	sendTimeout = 5 * time.Second
)

var (
	// ErrNotConnected is returned when not connected to a server
	ErrNotConnected = errors.New("not connected to a server")
	// ErrAlreadyClosed is returned when the connection is already closed
	ErrAlreadyClosed = errors.New("already closed: not connected to the server")
	// ErrShutdown is returned when already shutting down
	ErrShutdown = errors.New("session is shutting down")
	// ErrNack is returned when a message publish fails with a NACK
	ErrNack = errors.New("message was not sent")
	// ErrServerBusy is returned when there is too much tcp backpressure on a channel
	ErrServerBusy = errors.New("server busy; message was not sent")
	// ErrPublishOnly is returned when a channel is publish only and you try and use a queue function
	ErrPublishOnly = errors.New("channel is publish only")
	// ErrTimedOut is returned when a message times out waiting for confirmation
	ErrTimedOut = errors.New("confirmation timed out")
)

// New creates a new consumer state instance, and automatically
// attempts to connect to the server.
func New(logger log.Logger, config Config) *Session {
	if config.Name == "" {
		config.Name = pstrings.NewUUIDV4()
	}

	// set a default QoS
	if config.Qos <= 0 {
		config.Qos = 3
	}

	session := Session{
		logger:   log.With(logger, "pkg", "rabbitmq", "name", config.Name),
		config:   &config,
		done:     make(chan bool),
		messages: make(chan amqp.Delivery, 1),
	}
	// ensure our queue and binding are all setup
	session.ensureConsumerChannel()

	return &session
}

// init will initialize channel & declare queue
func (session *Session) ensureQueueAndBindings() error {
	if session.config.PublishOnly {
		return ErrPublishOnly
	}
	channel, err := session.config.ConsumerConnectionPool.GetTransientChannel(false)
	if err != nil {
		log.Error(session.logger, "error getting channel to create queue", "err", err)

	}
	log.Info(session.logger, "staring queue create", "queuename", session.config.Name)
	if _, err := channel.QueueDeclare(
		session.config.Name,
		session.config.DurableQueue, // Durable
		session.config.DeleteUnused, // Delete when unused
		session.config.Exclusive,    // Exclusive
		false,                       // No-wait
		session.config.Args,         // Arguments
	); err != nil {
		log.Error(session.logger, "error declaring queue", "err", err)
		channel.Close()
		return err
	}

	for _, key := range session.config.RoutingKeys {
		if err := session.consumerchannelhost.Channel.QueueBind(
			session.config.Name,     // queue name
			key,                     // routing key
			session.config.Exchange, // exchange
			false,
			nil,
		); err != nil {
			log.Error(session.logger, "error binding routing key", "err", err, "routingKey", key)
			channel.Close()

			return err
		}
	}

	log.Info(session.logger, "setup and ready")
	channel.Close()

	return nil
}

func (session *Session) ensurePublisherChannel() *ChannelHost {

	for {
		// Has to use an Ackable channel for Publish Confirmations.
		chanHost, err := session.config.PublisherConnectionPool.GetChannel()
		log.Debug(session.logger, "getting channel from pool to publish")

		if err != nil {
			log.Error(session.logger, "error with getting channel...retrying", "err", err)
			session.config.PublisherConnectionPool.ReturnChannel(chanHost, true) // this will just close the channel
			chanHost = nil
			time.Sleep(reInitDelay)
			continue
		} else {
			session.mu.Lock()
			session.publisherchannelhost = chanHost
			session.mu.Unlock()
			return chanHost
		}

	}
}

func (session *Session) ensureConsumerChannel() {

	for {
		// Has to use an Ackable channel for Publish Confirmations.
		chanHost, err := session.config.ConsumerConnectionPool.GetChannel()
		log.Debug(session.logger, "getting channel from pool to consume")

		if err != nil {
			log.Error(session.logger, "error with getting channel...retrying", "err", err)
			session.config.ConsumerConnectionPool.ReturnChannel(chanHost, true)
			chanHost = nil
			time.Sleep(reInitDelay)
			continue
		} else {
			session.mu.Lock()
			session.consumerchannelhost = chanHost
			// ensure our queue and binding are all setup
			prefetch := session.config.Qos
			session.mu.Unlock()

			// Configure RabbitMQ channel QoS for Consumer
			session.consumerchannelhost.Channel.Qos(prefetch, 0, false)
			session.ensureQueueAndBindings()

			return
		}
	}
}

// Push will publish data to channel
func (session *Session) Push(routingKey string, data amqp.Publishing) error {
	return session.config.PublisherConnectionPool.Push(session.config.Exchange, routingKey, data)
}

// Stream will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (session *Session) Stream(consumergroup string, autoAck bool, exclusive bool) (chan amqp.Delivery, error) {
	if session.config.PublishOnly {
		return nil, ErrPublishOnly
	}
	session.mu.Lock()
	session.autocommit = autoAck
	session.mu.Unlock()
	go session.startConsumeLoop(consumergroup, autoAck, exclusive)
	return session.messages, nil
}

// adapted from: https://github.com/houseofcat/turbocookedrabbit
func (session *Session) startConsumeLoop(consumergroup string, autoAck bool, exclusive bool) {
	log.Info(session.logger, "starting consume loop for", "consumer", consumergroup)

	for {
		log.Info(session.logger, "starting connection for", "consumer", consumergroup)

		// Initiate consuming process.
		deliveryChan, err := session.consumerchannelhost.Channel.Consume(
			session.config.Name,
			consumergroup, // Consumer
			autoAck,       // Auto-Ack
			exclusive,     // Exclusive
			false,         // No-local
			false,         // No-Wait
			nil,           // Args
		)
		log.Info(session.logger, "consuming", "consumer", consumergroup)

		if err != nil {
			if strings.Contains(err.Error(), "PRECONDITION_FAILED") {

				// try to cleanup
				ch, _ := session.config.ConsumerConnectionPool.GetTransientChannel(false)
				log.Warn(session.logger, "queue precondition failed, will try and delete and try again", "err", err, "queue", session.config.Name)
				ch.QueueDelete(session.config.Name, false, true, false)
				ch.Close()
				continue
			}
			session.config.ConsumerConnectionPool.ReturnChannel(session.consumerchannelhost, true)
			session.ensureConsumerChannel()
			log.Error(session.logger, "consumer's current channel closed", "err", err)
			continue
		}
		// Process delivered messages by the consumer, returns true when we are to stop all consuming.
		if session.processDeliveries(deliveryChan, session.consumerchannelhost) {
			log.Debug(session.logger, "process deliveries exited")

			err := session.consumerchannelhost.Channel.Cancel(
				consumergroup, // Consumer
				false,          // No-Wait
			)
			if err != nil {
				log.Error(session.logger, "error closing channel on exit", "err", err)
			}
			session.done <- true
			return
		}
		log.Error(session.logger, "error occurred processing deliveries, attempting to reconnect")
	}
}

// ProcessDeliveries is the inner loop for processing the deliveries and returns true to break outer loop.
// adapted from: https://github.com/houseofcat/turbocookedrabbit
func (session *Session) processDeliveries(deliveryChan <-chan amqp.Delivery, chanHost *ChannelHost) bool {

	for {
		// Listen for channel closure (close errors).
		select {
		case errorMessage := <-session.consumerchannelhost.Errors:
			if errorMessage != nil {
				log.Error(session.logger, "consumer's current channel errored...starting reconnect", "err", errorMessage, "reason", errorMessage.Reason, "code", errorMessage.Code)
				session.config.ConsumerConnectionPool.ReturnChannel(chanHost, true)

				session.ensureConsumerChannel()
				return false
			}

		case <-session.config.Context.Done():
			log.Debug(session.logger, "consumer context cancelled")
			return true
		case delivery := <-deliveryChan:
			// broker the amqp delivery channel through our session channel so we can surive a reconnect
			session.messages <- delivery

		}

	}
}

// Ack a consumer tag
func (session *Session) Ack(tag uint64) error {
	log.Debug(session.logger, "ack", "tag", tag, "autocommit", session.autocommit)

	session.mu.RLock()
	if session.autocommit {
		session.mu.RUnlock()
		return nil
	}
	session.mu.RUnlock()

	err := session.consumerchannelhost.Channel.Ack(tag, false)

	return err
}

// Nack a consumer tag
func (session *Session) Nack(tag uint64) error {

	log.Debug(session.logger, "nack", "tag", tag, "autocommit", session.autocommit)
	session.mu.RLock()
	if session.autocommit {
		session.mu.RUnlock()
		return nil
	}
	session.mu.RUnlock()

	// if we get here, this means `NACK` was sent manually through the socket, so requeue
	err := session.consumerchannelhost.Channel.Nack(tag, false, true)

	return err
}

// Close will cleanly shutdown the channel and connection.
func (session *Session) Close() error {
	var err error
	log.Debug(session.logger, "start shutdown of rabbit session")
	session.mu.Lock()
	// block until we've nacked anyting in flight
	<-session.done

	// session.config.PublisherConnectionPool.ReturnChannel(session.publisherchannelhost, false)
	session.config.ConsumerConnectionPool.ReturnChannel(session.consumerchannelhost, false)
	close(session.messages)
	close(session.done)
	session.consumerchannelhost = nil
	session.publisherchannelhost = nil
	session.mu.Unlock()
	log.Debug(session.logger, "...waiting for shutdown of rabbit session")
	log.Debug(session.logger, "rabbit session closed")

	return err
}
