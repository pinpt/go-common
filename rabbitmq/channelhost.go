package rabbitmq

// ported & adapted from: https://github.com/houseofcat/turbocookedrabbit for customizations in our stack
import (
	"errors"
	"sync"

	"github.com/pinpt/go-common/v10/log"
	"github.com/streadway/amqp"
)

// ChannelHost is an internal representation of amqp.Connection.
type ChannelHost struct {
	Channel       *amqp.Channel
	Confirmations chan amqp.Confirmation
	Errors        chan *amqp.Error
	Backpressure chan bool
	logger        log.Logger
	connHost      *ConnectionHost
	chanLock      *sync.Mutex
}

// NewChannelHost creates a simple ConnectionHost wrapper for management by end-user developer.
func NewChannelHost(
	logger log.Logger,
	connHost *ConnectionHost,
	ackable bool) (*ChannelHost, error) {
	if connHost.Connection.IsClosed() {
		return nil, errors.New("can't open a channel - connection is already closed")
	}

	chanHost := &ChannelHost{
		logger:   logger,
		connHost: connHost,
		chanLock: &sync.Mutex{},
	}

	err := chanHost.MakeChannel()
	if err != nil {
		return nil, err
	}

	return chanHost, nil
}

// Close allows for manual close of Amqp Channel kept internally.
func (ch *ChannelHost) Close() {
	ch.connHost.IncrChannels(-1)
	ch.Channel.Close()
}

// MakeChannel tries to create (or re-create) the channel from the ConnectionHost its attached to.
func (ch *ChannelHost) MakeChannel() (err error) {
	ch.chanLock.Lock()
	defer ch.chanLock.Unlock()

	ch.Channel, err = ch.connHost.Connection.Channel()
	if err != nil {
		return err
	}

	err = ch.Channel.Confirm(false)
	if err != nil {
		return err
	}

	ch.Confirmations = make(chan amqp.Confirmation, 1)
	ch.Channel.NotifyPublish(ch.Confirmations)

	ch.Errors = make(chan *amqp.Error, 1)
	ch.Channel.NotifyClose(ch.Errors)

	ch.Backpressure = make(chan bool, 1)
	ch.Channel.NotifyFlow(ch.Backpressure)

	ch.connHost.IncrChannels(1)

	return nil
}
