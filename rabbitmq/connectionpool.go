package rabbitmq

// ported & adapted from: https://github.com/houseofcat/turbocookedrabbit for customizations in our stack
import (
	"strconv"
	"sync"
	"time"

	"github.com/pinpt/go-common/v10/log"
	"github.com/streadway/amqp"
)

// ConnectionPool houses the pool of RabbitMQ connections.
type ConnectionPool struct {
	logger                   log.Logger
	name                     string
	uri                      string
	initialConnectionCount   int
	maxChannelsPerConnection int
	connections              chan *ConnectionHost
	publisherchannels        chan *ChannelHost
	connectionID             uint64
	poolRWLock               *sync.RWMutex
	flaggedConnections       map[uint64]bool
}

// NewConnectionPool creates hosting structure for the ConnectionPool.
func NewConnectionPool(logger log.Logger, baseConnectionName, uri string, connections, channelsPerConnection int) (*ConnectionPool, error) {

	cp := &ConnectionPool{
		logger:                   logger,
		name:                     baseConnectionName,
		uri:                      uri,
		initialConnectionCount:   connections,
		maxChannelsPerConnection: channelsPerConnection,
		connections:              make(chan *ConnectionHost, 2000), // TODO: add this to a metric and scale off it
		publisherchannels:        make(chan *ChannelHost, 5000),
		poolRWLock:               &sync.RWMutex{},
	}

	if err := cp.initializeConnections(); err != nil {
		return nil, err
	}

	return cp, nil
}

func (cp *ConnectionPool) createConnection(id int) (*ConnectionHost, error) {
	return NewConnectionHost(
		cp.logger,
		cp.uri,
		cp.name+"-"+strconv.Itoa(id))
}

func (cp *ConnectionPool) initializeConnections() error {

	for i := 0; i < cp.initialConnectionCount; i++ {
		connection, err := cp.createConnection(i)
		if err != nil {
			return err
		}
		cp.connections <- connection
	}

	return nil
}

// GetConnection gets a connection based on whats in the ConnectionPool or creates one to avoid blocking
func (cp *ConnectionPool) GetConnection() (*ConnectionHost, error) {
	cp.poolRWLock.RLock()
	conns := cp.connections
	cp.poolRWLock.RUnlock()
	maxcount := len(conns)
	i := 0
	for {
		if i == maxcount {
			log.Debug(cp.logger, "no free connection, creating new one")
			break
		}
		select {
		case conn := <-conns:

			if conn.CanTakeMoreChannels(cp.maxChannelsPerConnection) {
				return conn, nil
			}
			// put the connection back in the pool
			cp.connections <- conn
		default:
			break
		}
		i++
	}
	log.Debug(cp.logger, "creating new channel")

	conn, err := cp.createConnection(int(time.Now().Unix() * 1000))
	if err != nil {
		return nil, err
	}

	return conn, nil

}

// ReturnConnection puts the connection back in the queue
func (cp *ConnectionPool) ReturnConnection(connHost *ConnectionHost) {
	select {
	case cp.connections <- connHost:
		log.Debug(cp.logger, "closing connection")
	default:
		// pool is full, close passed connection
		connHost.Connection.Close()
	}
}

// GetChannel gets a ackable channel from the Pool if they exist or creates a channel.
func (cp *ConnectionPool) GetChannel() (*ChannelHost, error) {
	return cp.createChannel()

}

// ReturnChannel returns a Channel.
func (cp *ConnectionPool) ReturnChannel(chanHost *ChannelHost, erred bool) {
	chanHost.Close()
}

// createCacheChannel allows you create a cached ChannelHost which helps wrap Amqp Channel functionality.
func (cp *ConnectionPool) createChannel() (*ChannelHost, error) {

	// InfiniteLoop: Stay till we have a good channel.
	for {
		connHost, err := cp.GetConnection()
		log.Debug(cp.logger, "Got a connection to add a channel to")

		if err != nil || connHost.Connection.IsClosed() {
			log.Error(cp.logger, "error getting connection to create channel", "err", err)
			time.Sleep(reconnectDelay)
			continue
		}

		chanHost, err := NewChannelHost(cp.logger, connHost, true)
		log.Debug(cp.logger, "Got a new channel")

		if err != nil {
			log.Error(cp.logger, "error creating channel", "err", err)
			continue
		}
		log.Debug(cp.logger, "try to return connection")

		cp.ReturnConnection(connHost)
		log.Debug(cp.logger, "Returning connection")

		return chanHost, nil
	}
}

func (cp *ConnectionPool) getPublisherChannel() (*ChannelHost, error) {
	select {
	case publisher := <-cp.publisherchannels:
		log.Debug(cp.logger, "got publisher channel from pool")

		return publisher, nil
	default:
		log.Debug(cp.logger, "pool exhausted, creating publisher channel")

		publisher, err := cp.GetChannel()

		return publisher, err
	}
}

// returnPublisherChannel puts the channel back in the queue
func (cp *ConnectionPool) returnPublisherChannel(publisher *ChannelHost) {
	select {
	case cp.publisherchannels <- publisher:
		log.Debug(cp.logger, "putting publisher channel")
	default:
		// pool is full, close passed channel
		publisher.Close()
	}
}

// GetTransientChannel allows you create an unmanaged amqp Channel with the help of the ConnectionPool.
func (cp *ConnectionPool) GetTransientChannel(ackable bool) (*amqp.Channel, error) {
	// InfiniteLoop: Stay till we have a good channel.
	for {
		connHost, err := cp.GetConnection()
		if err != nil {
			log.Error(cp.logger, "error occurered getting connection for transient channel", "err", err)
			continue
		}

		channel, err := connHost.Connection.Channel()
		if err != nil {
			log.Error(cp.logger, "error occurered creating channel for transient channel", "err", err)
			cp.ReturnConnection(connHost)
			continue
		}

		cp.ReturnConnection(connHost)

		if ackable {
			err := channel.Confirm(false)
			if err != nil {
				log.Error(cp.logger, "error occurered creating ackable channel for transient channel", "err", err)
				continue
			}
		}
		return channel, nil
	}
}

func (cp *ConnectionPool) ensurePublisherChannel() *ChannelHost {

	for {
		// Has to use an Ackable channel for Publish Confirmations.
		chanHost, err := cp.getPublisherChannel()
		log.Debug(cp.logger, "getting channel from pool to publish")

		if err != nil {
			log.Error(cp.logger, "error with getting channel...retrying", "err", err)
			cp.ReturnChannel(chanHost, true) // this will just close the channel
			chanHost = nil
			continue
		} else {
			return chanHost
		}

	}
}

// PushWithRetry sends data using the publisher channel pool
func (cp *ConnectionPool) PushWithRetry(exchange, routingKey string, data amqp.Publishing) error {
	var err error
	retries := 3
	count := 0
	publisherchannelhost := cp.ensurePublisherChannel()
	for {
		count++
		select {
		// if we get backpressure from the publisher socket, treat this as a reconnect situation
		case <-publisherchannelhost.Backpressure:
			log.Warn(cp.logger, "got backpressure from rabbit server, getting a new connection", "retrynumber", count)
			cp.ReturnChannel(publisherchannelhost, true) //just close the channel

			publisherchannelhost = cp.ensurePublisherChannel()
			if count <= retries {
				continue
			} else {
				return ErrServerBusy
			}
		default:
		}

		log.Debug(cp.logger, "trying to publish")
		err = publisherchannelhost.Channel.Publish(
			exchange,   // Exchange
			routingKey, // Routing key
			false,      // Mandatory
			false,      // Immediate
			data,
		)
		if err != nil {
			log.Error(cp.logger, "publisher's current channel errored. Retrying with a new channel", "retrynumber", count, "err", err)
			cp.ReturnChannel(publisherchannelhost, true) //just close the channel
			publisherchannelhost = cp.ensurePublisherChannel()
			if count <= retries {
				continue
			}
			return ErrNotConnected
		}

		// to make this a reliable message publishing method we need to wait for a
		// confirmation from the broker that the message was either confirmed or timed out
		select {
		case err := <-publisherchannelhost.Errors:
			log.Error(cp.logger, "publisher's current channel errored. Retrying with a new channel", "retrynumber", count, "err", err)
			cp.ReturnChannel(publisherchannelhost, true) //just close the channel
			publisherchannelhost = cp.ensurePublisherChannel()
			if count <= retries {
				continue
			}

			return ErrNotConnected
		case confirmed := <-publisherchannelhost.Confirmations:
			if confirmed.Ack {
				log.Debug(cp.logger, "push ack", "routingKey", routingKey, "message_id", data.MessageId, "data", data)
				cp.returnPublisherChannel(publisherchannelhost)

				return nil
			}
			log.Debug(cp.logger, "push failed with nack", "routingKey", routingKey, "message_id", data.MessageId)
			if count <= retries {
				continue
			}
			cp.returnPublisherChannel(publisherchannelhost)
			return ErrNack

		case <-time.After(sendTimeout):
			log.Debug(cp.logger, "timed out waiting for ack", "retrynumber", count, "routingKey", routingKey, "message_id", data.MessageId)
			if count <= retries {
				continue
			}
			cp.ReturnChannel(publisherchannelhost, true) //just close the channel
			return ErrTimedOut
		}

	}
}

// Push sends data using the publisher channel pool
func (cp *ConnectionPool) Push(exchange, routingKey string, data amqp.Publishing) error {
	ts := time.Now()
	var err error
	count := 0
	log.Debug(cp.logger, "getting a channel to publish on", "message_id", data.MessageId, "duration", time.Since(ts))

	publisherchannelhost := cp.ensurePublisherChannel()

	log.Debug(cp.logger, "trying to publish", "message_id", data.MessageId, "duration", time.Since(ts))
	err = publisherchannelhost.Channel.Publish(
		exchange,   // Exchange
		routingKey, // Routing key
		false,      // Mandatory
		false,      // Immediate
		data,
	)
	if err != nil {
		log.Error(cp.logger, "publisher's current channel errored. Retrying with a new channel", "retrynumber", count, "err", err, "message_id", data.MessageId)
		cp.ReturnChannel(publisherchannelhost, true) //just close the channel
		return ErrNotConnected
	}
	log.Debug(cp.logger, "message sent waiting for ack", "message_id", data.MessageId, "duration", time.Since(ts))

	// to make this a reliable message publishing method we need to wait for a
	// confirmation from the broker that the message was either confirmed or timed out
	select {
	case confirmed := <-publisherchannelhost.Confirmations:
		if confirmed.Ack {
			log.Debug(cp.logger, "push ack", "routingKey", routingKey, "message_id", data.MessageId, "duration", time.Since(ts))
			cp.returnPublisherChannel(publisherchannelhost)

			return nil
		}
	case <-time.After(sendTimeout):
		log.Debug(cp.logger, "timed out waiting for ack", "routingKey", routingKey, "message_id", data.MessageId, "duration", time.Since(ts))
		cp.ReturnChannel(publisherchannelhost, true) //just close the channel

		return ErrTimedOut
	}
	log.Debug(cp.logger, "push failed with nack", "routingKey", routingKey, "message_id", data.MessageId)
	cp.ReturnChannel(publisherchannelhost, true) //just close the channel

	return ErrNack
}
