package rabbitmq

import (
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pinpt/go-common/log"
	pstrings "github.com/pinpt/go-common/strings"
	"github.com/streadway/amqp"
)

var debug = os.Getenv("PP_RABBIT_DEBUG") == "1"

// Config for the session
type Config struct {
	Name         string
	Exchange     string
	Addr         string
	DurableQueue bool
	DeleteUnused bool
	Exclusive    bool
	Args         amqp.Table
	Qos          int
	PublishOnly  bool
}

// Session is the rabbitmq session
type Session struct {
	logger          log.Logger
	config          Config
	name            string
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyPublish   chan amqp.Confirmation
	isReady         bool
	isInit          bool
	notifyInit      chan bool
	mu              sync.Mutex
	bindings        []string
	autocommit      bool
}

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second

	// When setting up the channel after a channel exception
	reInitDelay = 2 * time.Second

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
	session := Session{
		logger:     log.With(logger, "pkg", "rabbitmq", "name", config.Name),
		config:     config,
		done:       make(chan bool),
		notifyInit: make(chan bool),
	}
	go session.handleReconnect(config.Addr)
	<-session.notifyInit // wait for init
	return &session
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (session *Session) handleReconnect(addr string) {
	for {
		session.mu.Lock()
		session.isReady = false
		if debug {
			log.Debug(session.logger, "attempting to connect", "addr", addr)
		}

		conn, err := session.connect(addr)
		session.mu.Unlock()

		if err != nil {
			log.Error(session.logger, "Failed to connect. Retrying...", "addr", addr, "err", err)

			select {
			case <-session.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		if done := session.handleReInit(conn); done {
			break
		}
	}
}

// connect will create a new AMQP connection
func (session *Session) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)

	if err != nil {
		return nil, err
	}

	session.changeConnection(conn)
	if debug {
		log.Debug(session.logger, "connected", "addr", addr)
	}
	return conn, nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (session *Session) handleReInit(conn *amqp.Connection) bool {
	for {
		session.mu.Lock()
		session.isReady = false
		session.mu.Unlock()

		err := session.init(conn)

		if err != nil {
			select {
			case <-session.done:
				return true
			case <-time.After(reInitDelay):
			}
			if strings.Contains(err.Error(), "PRECONDITION_FAILED") {
				ch, err := conn.Channel()
				if err == nil {
					log.Warn(session.logger, "queue precondition failed, will try and delete and try again", "err", err, "queue", session.config.Name)
					ch.QueueDelete(session.config.Name, false, true, false)
					ch.Close()
					continue
				}
			}
			log.Error(session.logger, "failed to initialize channel, retrying...", "err", err)
			continue
		}

		session.mu.Lock()
		if !session.isInit {
			session.isInit = true
			session.notifyInit <- true
		}
		session.mu.Unlock()

		select {
		case <-session.done:
			return true
		case err := <-session.notifyConnClose:
			log.Error(session.logger, "Connection closed. Reconnecting...", "err", err)
			return false
		case err := <-session.notifyChanClose:
			select {
			case <-session.done:
				return true
			default:
			}
			log.Error(session.logger, "Channel closed. Re-running init...", "err", err)
		}
	}
}

// init will initialize channel & declare queue
func (session *Session) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	if !session.config.PublishOnly {
		_, err = ch.QueueDeclare(
			session.config.Name,
			session.config.DurableQueue, // Durable
			session.config.DeleteUnused, // Delete when unused
			session.config.Exclusive,    // Exclusive
			false,                       // No-wait
			session.config.Args,         // Arguments
		)

		if err != nil {
			return err
		}
	}

	session.changeChannel(ch)
	session.isReady = true
	if debug {
		log.Debug(session.logger, "setup and ready")
	}
	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (session *Session) changeConnection(connection *amqp.Connection) {
	session.connection = connection
	session.notifyConnClose = make(chan *amqp.Error)
	session.connection.NotifyClose(session.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (session *Session) changeChannel(channel *amqp.Channel) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.channel = channel
	session.notifyChanClose = make(chan *amqp.Error, 1)
	session.notifyPublish = make(chan amqp.Confirmation, 1)
	channel.NotifyClose(session.notifyChanClose)
	channel.NotifyPublish(session.notifyPublish)
	if err := channel.Confirm(false); err != nil {
		log.Fatal(session.logger, "channel confirm failed", "err", err)
	}
	prefetch := session.config.Qos
	if prefetch <= 0 {
		prefetch = 3
	}
	if err := session.channel.Qos(prefetch, 0, false); err != nil {
		log.Fatal(session.logger, "error setting channel qos", "err", err)
	}
	for _, key := range session.bindings {
		if err := session.bind(key); err != nil {
			log.Fatal(session.logger, "error binding routing key", "err", err, "routingKey", key)
		}
	}
}

func (session *Session) bind(routingKey string) error {
	return session.channel.QueueBind(
		session.config.Name,     // queue name
		routingKey,              // routing key
		session.config.Exchange, // exchange
		false,
		nil,
	)
}

// Bind the queue to a routingKey
func (session *Session) Bind(routingKey string) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	ready := session.isReady
	if !ready {
		return ErrNotConnected
	}
	if session.config.PublishOnly {
		return ErrPublishOnly
	}
	session.bindings = append(session.bindings, routingKey)
	return session.bind(routingKey)
}

// Push will publish data to channel
func (session *Session) Push(routingKey string, data amqp.Publishing) error {
	ts := time.Now()
	session.mu.Lock()
	ready := session.isReady
	session.mu.Unlock()
	if !ready {
		return ErrNotConnected
	}
	if debug {
		log.Debug(session.logger, "push", "routingKey", routingKey, "message_id", data.MessageId)
	}
	if err := session.channel.Publish(
		session.config.Exchange, // Exchange
		routingKey,              // Routing key
		false,                   // Mandatory
		false,                   // Immediate
		data,
	); err != nil {
		return err
	}
	// to make this a reliable message publishing method we need to wait for a
	// confirmation from the broker that the message was either confirmed or timed out
	select {
	case confirmed := <-session.notifyPublish:
		if confirmed.Ack {
			if debug {
				log.Debug(session.logger, "push ack", "routingKey", routingKey, "message_id", data.MessageId, "duration", time.Since(ts))
			}
			return nil
		}
	case <-time.After(sendTimeout):
		if debug {
			log.Debug(session.logger, "timed out waiting for ack", "routingKey", routingKey, "message_id", data.MessageId, "duration", time.Since(ts))
		}
		return ErrTimedOut
	}
	if debug {
		log.Debug(session.logger, "push failed with nack", "routingKey", routingKey, "message_id", data.MessageId)
	}
	return ErrNack
}

// Stream will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (session *Session) Stream(consumer string, autoAck bool, exclusive bool) (<-chan amqp.Delivery, error) {
	session.mu.Lock()
	ready := session.isReady
	session.name = consumer
	session.autocommit = autoAck
	session.mu.Unlock()
	if !ready {
		return nil, ErrNotConnected
	}
	if session.config.PublishOnly {
		return nil, ErrPublishOnly
	}
	return session.channel.Consume(
		session.config.Name,
		consumer,  // Consumer
		autoAck,   // Auto-Ack
		exclusive, // Exclusive
		false,     // No-local
		false,     // No-Wait
		nil,       // Args
	)
}

// Ack a consumer tag
func (session *Session) Ack(tag uint64) error {
	session.mu.Lock()
	if debug {
		log.Debug(session.logger, "ack", "tag", tag, "autocommit", session.autocommit)
	}
	if session.autocommit {
		session.mu.Unlock()
		return nil
	}
	ready := session.isReady
	session.mu.Unlock()
	if !ready {
		return ErrNotConnected
	}
	return session.channel.Ack(tag, false)
}

// Close will cleanly shutdown the channel and connection.
func (session *Session) Close() error {
	if debug {
		log.Debug(session.logger, "closing")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if !session.isReady {
		return ErrAlreadyClosed
	}
	close(session.done)
	session.isReady = false
	session.channel.Cancel(session.name, true)
	err := session.channel.Close()
	if err != nil {
		return err
	}
	err = session.connection.Close()
	if err != nil {
		return err
	}
	if debug {
		log.Debug(session.logger, "closed")
	}
	return nil
}
