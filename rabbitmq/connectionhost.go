package rabbitmq

// ported & adapted from: https://github.com/houseofcat/turbocookedrabbit for customizations in our stack
import (
	"github.com/pinpt/go-common/v10/log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// ConnectionHost is an internal representation of amqp.Connection.
type ConnectionHost struct {
	Connection     *amqp.Connection
	logger         log.Logger
	uri            string
	connectionName string
	Errors         chan *amqp.Error
	connLock       *sync.RWMutex
	channelcount   int
}

// NewConnectionHost creates a simple ConnectionHost wrapper for management by end-user developer.
func NewConnectionHost(
	logger log.Logger,
	uri string,
	connectionName string,
) (*ConnectionHost, error) {

	connHost := &ConnectionHost{
		uri:            uri,
		connectionName: connectionName,
		Errors:         make(chan *amqp.Error),
		connLock:       &sync.RWMutex{},
	}

	err := connHost.Connect()
	if err != nil {
		return nil, err
	}

	return connHost, nil
}

// IncrChannels increments the channel count by i (use -1 for decr)
func (ch *ConnectionHost) IncrChannels(i int) {
	ch.connLock.Lock()
	ch.channelcount = ch.channelcount + i
	ch.connLock.Unlock()

}
// CanTakeMoreChannels is a helpermethod to see if a connection can take another channel
func (ch *ConnectionHost) CanTakeMoreChannels(maxchannels int) bool {
	ch.connLock.RLock()
	defer ch.connLock.RUnlock()
	if ch.channelcount < maxchannels {
		return true
	}
	return false
}

// Connect tries to connect (or reconnect) to the provided properties of the host one time.
func (ch *ConnectionHost) Connect() error {

	// Compare, Lock, Recompare Strategy
	if ch.Connection != nil && !ch.Connection.IsClosed() {
		return nil
	}

	ch.connLock.Lock() // Block all but one.
	defer ch.connLock.Unlock()

	// Recompare, check if an operation is still necessary after acquiring lock.
	if ch.Connection != nil && !ch.Connection.IsClosed() {
		return nil
	}

	// Proceed with reconnectivity
	var amqpConn *amqp.Connection
	var err error

	amqpConn, err = amqp.Dial(ch.uri)

	if err != nil {
		return err
	}

	ch.Connection = amqpConn
	ch.Errors = make(chan *amqp.Error, 10)

	ch.Connection.NotifyClose(ch.Errors) // ch.Errors is closed by streadway/amqp in some scenarios :(
	ch.VerifyConnection()

	return nil
}

// VerifyConnection will make sure a connection is ok or try to reconnect it
func (ch *ConnectionHost) VerifyConnection() {

	healthy := true
	select {
	case err := <-ch.Errors:
		log.Error(ch.logger, "an error occured in the connection...", "err", err)
		healthy = false
	default:
		break
	}

	// Between these three states we do our best to determine that a connection is dead in the various lifecycles.
	if !healthy || ch.Connection.IsClosed( /* atomic */ ) {
		ch.triggerConnectionRecovery()
	}
}
func (ch *ConnectionHost) triggerConnectionRecovery() {

	// InfiniteLoop: Stay here till we reconnect.
	for {
		err := ch.Connect()
		if err != nil {
			log.Error(ch.logger, "error occured during reconnection...retrying")
			time.Sleep(reconnectDelay)
			continue
		}
		break
	}

	// Flush any pending errors.
	for {
		select {
		case <-ch.Errors:
		default:
			return
		}
	}
}
