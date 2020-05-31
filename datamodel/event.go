package datamodel

import (
	"time"

	"github.com/pinpt/go-common/eventing"
)

type modelSendEvent struct {
	object  StreamedModel
	headers map[string]string
}

var _ ModelSendEvent = (*modelSendEvent)(nil)

// Key is the key to use for the message
func (o *modelSendEvent) Key() string {
	cfg := o.object.GetTopicConfig()
	if cfg != nil {
		return o.object.GetTopicKey()
	}
	return o.object.GetID()
}

// Object returns an instance of the Model that will be send
func (o *modelSendEvent) Object() StreamedModel {
	return o.object
}

// Headers returns any headers for the event. can be nil to not send any additional headers
func (o *modelSendEvent) Headers() map[string]string {
	return o.headers
}

// Timestamp returns the event timestamp. If empty, will default to time.Now()
func (o *modelSendEvent) Timestamp() time.Time {
	return o.object.GetTimestamp()
}

// NewModelSendEvent will send just a model object
func NewModelSendEvent(object StreamedModel) ModelSendEvent {
	return &modelSendEvent{object, nil}
}

// NewModelSendEventWithHeaders will send just a model object and headers
func NewModelSendEventWithHeaders(object StreamedModel, headers map[string]string) ModelSendEvent {
	return &modelSendEvent{object, headers}
}

type modelReceiveEvent struct {
	object StreamedModel
	msg    eventing.Message
	eof    bool
}

var _ ModelReceiveEvent = (*modelReceiveEvent)(nil)

// Object returns an instance of the Model that was received
func (o *modelReceiveEvent) Object() StreamedModel {
	return o.object
}

// Message returns the underlying message data for the event
func (o *modelReceiveEvent) Message() eventing.Message {
	return o.msg
}

// EOF returns true if an EOF event was received. in this case, the Object and Message will return nil
func (o *modelReceiveEvent) EOF() bool {
	return o.eof
}

// NewModelReceiveEvent returns a new ModelReceiveEvent
func NewModelReceiveEvent(msg eventing.Message, obj StreamedModel) ModelReceiveEvent {
	return &modelReceiveEvent{obj, msg, false}
}

// NewModelReceiveEventEOF creates a model receive event that signals an EOF on the topic
func NewModelReceiveEventEOF(msg eventing.Message) ModelReceiveEvent {
	return &modelReceiveEvent{nil, msg, true}
}
