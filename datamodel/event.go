package datamodel

import (
	"time"

	"github.com/pinpt/go-common/eventing"
)

type modelSendEvent struct {
	object Model
}

var _ ModelSendEvent = (*modelSendEvent)(nil)

// Key is the key to use for the message
func (o *modelSendEvent) Key() string {
	return o.object.GetID()
}

// Object returns an instance of the Model that will be send
func (o *modelSendEvent) Object() Model {
	return o.object
}

// Headers returns any headers for the event. can be nil to not send any additional headers
func (o *modelSendEvent) Headers() map[string]string {
	return nil
}

// Timestamp returns the event timestamp. If empty, will default to time.Now()
func (o *modelSendEvent) Timestamp() time.Time {
	return time.Now()
}

func NewModelSendEvent(object Model) ModelSendEvent {
	return &modelSendEvent{object}
}

type modelReceiveEvent struct {
	object Model
	msg    eventing.Message
}

var _ ModelReceiveEvent = (*modelReceiveEvent)(nil)

// Object returns an instance of the Model that was received
func (o *modelReceiveEvent) Object() Model {
	return o.object
}

// Message returns the underlying message data for the event
func (o *modelReceiveEvent) Message() eventing.Message {
	return o.msg
}
