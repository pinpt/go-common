package datamodel

// EventAPIPublish has details about what the publish rules are for event api
type EventAPIPublish struct {
	Public bool
}

// EventAPISubscribe has details about what the subscribe rules are for event api
type EventAPISubscribe struct {
	Public bool
	Key    string
}

// EventAPIConfig contains the configure for both publish and subscribe
type EventAPIConfig struct {
	Publish   EventAPIPublish
	Subscribe EventAPISubscribe
}

// EventAPIAuthorization is a type for defining the authorization level
type EventAPIAuthorization int

const (
	// EventAPIAuthorizationNone is a public (not apikey or shared secret)
	EventAPIAuthorizationNone EventAPIAuthorization = iota
	// EventAPIAuthorizationNone is an internal service-to-service authorization
	EventAPIAuthorizationInternal
	// EventAPIAuthorizationAPIKey is an external api key authorization
	EventAPIAuthorizationAPIKey
)

// CanSubscribeEventAPI returns true if the subscription can be sent
func CanSubscribeEventAPI(auth EventAPIAuthorization, customerID string, headers map[string]string, event ModelReceiveEvent) bool {
	if auth == EventAPIAuthorizationInternal {
		return true // internal can always subscribe
	}
	if c, ok := event.Object().(EventAPI); ok {
		switch auth {
		case EventAPIAuthorizationAPIKey:
			// if using an apikey, you can susbcribe as long as it matches the customer id on the header
			if customerID == event.Message().Headers["customer_id"] {
				return true
			}
		case EventAPIAuthorizationNone:
			cfg := c.GetEventAPIConfig()
			// if public, make sure that you can only subscribe if the event api config is marked as public
			if cfg.Subscribe.Public {
				// also need to check the header for the configured key
				val := event.Message().Headers[cfg.Subscribe.Key]
				if val != "" {
					// the incoming header for subscribe has to match the outgoing message
					return headers[cfg.Subscribe.Key] == val
				}
			}
		}
	}
	// if not marked with publish interface, assume no
	return false
}

// CanPublishEventAPI returns true if the event can be published for a given customerID and authorization
func CanPublishEventAPI(auth EventAPIAuthorization, customerID string, event ModelSendEvent) bool {
	if auth == EventAPIAuthorizationInternal {
		return true // internal can always publish
	}
	if c, ok := event.Object().(EventAPI); ok {
		switch auth {
		case EventAPIAuthorizationAPIKey:
			// if using an apikey, you can publish as long as it matches the customer id on the header
			if customerID == event.Headers()["customer_id"] {
				return true
			}
		case EventAPIAuthorizationNone:
			// if public, make sure that you can only publish if the event api config is marked as public
			return c.GetEventAPIConfig().Publish.Public
		}
	}
	// if not marked with publish interface, assume no
	return false
}

// EventAPI is an interface that models can implement if they have event api config
type EventAPI interface {
	// GetEventAPIConfig returns the EventAPIConfig
	GetEventAPIConfig() EventAPIConfig
}
