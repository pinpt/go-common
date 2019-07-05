package action

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/pinpt/go-common/datamodel"
	"github.com/pinpt/go-common/event"
)

// ModelFactory creates new instances of models
type ModelFactory interface {
	New(name datamodel.ModelNameType) datamodel.Model
}

type Config struct {
	// GroupID is the consumer group id
	GroupID string
	// Channel to use when subscribing
	Channel string
	// APIKey to use when subscribing
	APIKey string
	// Headers to use when subscribing
	Headers map[string]string
	// Topic to use when subscribing
	Topic string
	// Errors is a channel for writing any errors during processing
	Errors chan<- error
	// Factory is a model factory if you need to modify to use a different once
	Factory ModelFactory
	// Offset controls where to start reading from. if not provided, will be from the latest
	Offset string
}

// Action defines a specific action interface for running an action in response to an event
type Action interface {
	// Execute to invoke when a message is received. Return nil to not send a response or return an instance to send in response
	Execute(instance datamodel.Model) (datamodel.Model, error)
}

type ActionFunc func(instance datamodel.Model) (datamodel.Model, error)

type action struct {
	callback ActionFunc
}

func (a *action) Execute(instance datamodel.Model) (datamodel.Model, error) {
	return a.callback(instance)
}

// NewAction is a convenient wrapper that implements the Action interface
func NewAction(callback ActionFunc) Action {
	return &action{callback}
}

// ActionSubscription is returned to control when the subscription should be closed
type ActionSubscription struct {
	ctx          context.Context
	action       Action
	subscription *event.SubscriptionChannel
	config       Config
	mu           sync.Mutex
}

// Close should be called to stop receiving data from event server
func (s *ActionSubscription) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subscription.Close()
}

func (s *ActionSubscription) run() {
	for e := range s.subscription.Channel() {
		// create a new instance
		instance := s.config.Factory.New(datamodel.ModelNameType(e.Model))
		if instance == nil {
			s.config.Errors <- fmt.Errorf("no model returned from the factory for %v", e.Model)
			return
		}
		// deserialize the data into the instance
		switch e.Type {
		case "avro":
			instance.FromAvroBinary([]byte(e.Data))
		case "json":
			kv := make(map[string]interface{})
			if err := json.Unmarshal([]byte(e.Data), &kv); err != nil {
				s.config.Errors <- fmt.Errorf("error parsing json data for %v: %v", e.Model, err)
				return
			}
			instance.FromMap(kv)
		default:
			s.config.Errors <- fmt.Errorf("no data type for %v", e.Model)
			return
		}
		// run the action
		result, err := s.action.Execute(instance)
		if err != nil {
			s.config.Errors <- fmt.Errorf("error running action for %v: %v", e.Model, err)
			return
		}
		// if we have a result, publish the result
		if result != nil {
			if err := event.Publish(s.ctx, event.PublishEvent{Object: result, Headers: s.config.Headers}, s.config.Channel, s.config.APIKey); err != nil {
				s.config.Errors <- fmt.Errorf("error sending response for action %v: %v", e.Model, err)
				return
			}
		}
	}
}

// Register an action and return a subscription. You must call Close on the response when you're done (or shutting down)
func Register(ctx context.Context, action Action, config Config) (*ActionSubscription, error) {
	subscription := event.Subscription{
		GroupID: config.GroupID,
		Topics:  []string{config.Topic},
		Headers: config.Headers,
		Channel: config.Channel,
		APIKey:  config.APIKey,
		Errors:  config.Errors,
		Offset:  config.Offset,
	}
	ch, err := event.NewSubscription(ctx, subscription)
	if err != nil {
		return nil, err
	}
	sub := &ActionSubscription{
		ctx:          ctx,
		action:       action,
		subscription: ch,
		config:       config,
	}
	go sub.run()
	return sub, nil
}
