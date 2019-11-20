package action

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/pinpt/go-common/datamodel"
	"github.com/pinpt/go-common/datetime"
	"github.com/pinpt/go-common/event"
	"github.com/pinpt/go-common/hash"
	pjson "github.com/pinpt/go-common/json"
	"github.com/pinpt/go-common/number"
	pstrings "github.com/pinpt/go-common/strings"
	"github.com/stretchr/testify/assert"
)

const (
	// TrackTopic is the default topic name
	TrackTopic datamodel.TopicNameType = "web_Track_topic"

	// TrackStream is the default stream name
	TrackStream datamodel.TopicNameType = "web_Track_stream"

	// TrackTable is the default table name
	TrackTable datamodel.TopicNameType = "web_Track"

	// TrackModelName is the model name
	TrackModelName datamodel.ModelNameType = "web.Track"
)

// Track track is an event for tracking actions
type Track struct {
	// Action the action of the track event (the verb)
	Action string `json:"action" bson:"action" yaml:"action" faker:"-"`
	// CalculatedDate the date when the track was calculated
	// CalculatedDate TrackCalculatedDate `json:"calculated_date" bson:"calculated_date" yaml:"calculated_date" faker:"-"`
	// CustomerID the customer id for the track event
	CustomerID *string `json:"customer_id" bson:"customer_id" yaml:"customer_id" faker:"-"`
	// DeviceID the unique device id
	DeviceID string `json:"device_id" bson:"device_id" yaml:"device_id" faker:"-"`
	// Event the name of the track event (the noun)
	Event string `json:"event" bson:"event" yaml:"event" faker:"-"`
	// ID the primary key for this model instance
	ID string `json:"id" bson:"_id" yaml:"id" faker:"-"`
	// IP the ip address for the track event
	IP string `json:"ip" bson:"ip" yaml:"ip" faker:"-"`
	// Page the full page details for the track event
	// Page TrackPage `json:"page" bson:"page" yaml:"page" faker:"-"`
	// Properties the properties of the track event
	Properties map[string]string `json:"properties" bson:"properties" yaml:"properties" faker:"-"`
	// SessionID the unique session id
	SessionID string `json:"session_id" bson:"session_id" yaml:"session_id" faker:"-"`
	// UpdatedAt the timestamp that the model was last updated fo real
	UpdatedAt int64 `json:"updated_ts" bson:"updated_ts" yaml:"updated_ts" faker:"-"`
	// UserID the user id for the track event
	UserID *string `json:"user_id" bson:"user_id" yaml:"user_id" faker:"-"`
	// Useragent the user agent for the track event
	Useragent string `json:"useragent" bson:"useragent" yaml:"useragent" faker:"-"`
	// Hashcode stores the hash of the value of this object whereby two objects with the same hashcode are functionality equal
	Hashcode string `json:"hashcode" bson:"hashcode" yaml:"hashcode" faker:"-"`
}

// ensure that this type implements the data model interface
var _ datamodel.Model = (*Track)(nil)

// String returns a string representation of Track
func (o *Track) String() string {
	return fmt.Sprintf("web.Track<%s>", o.ID)
}

// GetTopicName returns the name of the topic if evented
func (o *Track) GetTopicName() datamodel.TopicNameType {
	return TrackTopic
}

// GetTableName returns the name of the topic if evented
func (o *Track) GetTableName() string {
	return ""
}

// GetTopicName returns the name of the topic if evented
func (o *Track) GetStreamName() string {
	return TrackStream.String()
}

// GetModelName returns the name of the model
func (o *Track) GetModelName() datamodel.ModelNameType {
	return TrackModelName
}

var emptyString string

func (o *Track) setDefaults(frommap bool) {
	if o.CustomerID == nil {
		o.CustomerID = &emptyString
	}
	if o.UserID == nil {
		o.UserID = &emptyString
	}

	if o.ID == "" {
		o.ID = hash.Values(o.DeviceID, o.SessionID, o.Event, o.Action)
	}

	if frommap {
		o.FromMap(map[string]interface{}{})
	}
}

// GetID returns the ID for the object
func (o *Track) GetID() string {
	return o.ID
}

// GetTopicKey returns the topic message key when sending this model as a ModelSendEvent
func (o *Track) GetTopicKey() string {
	var i interface{} = o.DeviceID
	if s, ok := i.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", i)
}

// GetTimestamp returns the timestamp for the model or now if not provided
func (o *Track) GetTimestamp() time.Time {
	var dt interface{} = o.UpdatedAt
	switch v := dt.(type) {
	case int64:
		return datetime.DateFromEpoch(v).UTC()
	case string:
		tv, err := datetime.ISODateToTime(v)
		if err != nil {
			panic(err)
		}
		return tv.UTC()
	case time.Time:
		return v.UTC()
	}
	panic("not sure how to handle the date time format for Track")
}

// IsMaterialized returns true if the model is materialized
func (o *Track) IsMaterialized() bool {
	return false
}

// GetModelMaterializeConfig returns the materialization config if materialized or nil if not
func (o *Track) GetModelMaterializeConfig() *datamodel.ModelMaterializeConfig {
	return nil
}

// IsEvented returns true if the model supports eventing and implements ModelEventProvider
func (o *Track) IsEvented() bool {
	return true
}

// IsMutable returns true if the model is mutable
func (o *Track) IsMutable() bool {
	return true
}

// SetEventHeaders will set any event headers for the object instance
func (o *Track) SetEventHeaders(kv map[string]string) {
	kv["model"] = TrackModelName.String()
}

// GetTopicConfig returns the topic config object
func (o *Track) GetTopicConfig() *datamodel.ModelTopicConfig {
	retention, err := time.ParseDuration("168h0m0s")
	if err != nil {
		panic("Invalid topic retention duration provided: 168h0m0s. " + err.Error())
	}

	ttl, err := time.ParseDuration("0s")
	if err != nil {
		ttl = 0
	}
	if ttl == 0 && retention != 0 {
		ttl = retention // they should be the same if not set
	}
	return &datamodel.ModelTopicConfig{
		Key:               "device_id",
		Timestamp:         "updated_ts",
		NumPartitions:     8,
		ReplicationFactor: 3,
		Retention:         retention,
		MaxSize:           5242880,
		TTL:               ttl,
	}
}

// GetCustomerID will return the customer_id
func (o *Track) GetCustomerID() string {
	if o.CustomerID == nil {
		return ""
	}
	return *o.CustomerID

}

// Clone returns an exact copy of Track
func (o *Track) Clone() datamodel.Model {
	c := new(Track)
	c.FromMap(o.ToMap())
	return c
}

// Anon returns the data structure as anonymous data
func (o *Track) Anon() datamodel.Model {
	return o
}

// MarshalJSON returns the bytes for marshaling to json
func (o *Track) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.ToMap())
}

// UnmarshalJSON will unmarshal the json buffer into the object
func (o *Track) UnmarshalJSON(data []byte) error {
	kv := make(map[string]interface{})
	if err := json.Unmarshal(data, &kv); err != nil {
		return err
	}
	o.FromMap(kv)
	if idstr, ok := kv["id"].(string); ok {
		o.ID = idstr
	}
	return nil
}

// Stringify returns the object in JSON format as a string
func (o *Track) Stringify() string {
	return pjson.Stringify(o)
}

// IsEqual returns true if the two Track objects are equal
func (o *Track) IsEqual(other *Track) bool {
	return o.GetID() == other.GetID()
}

// ToMap returns the object as a map
func (o *Track) ToMap() map[string]interface{} {
	o.setDefaults(false)
	return map[string]interface{}{
		"action":      o.Action,
		"customer_id": o.CustomerID,
		"device_id":   o.DeviceID,
		"event":       o.Event,
		"id":          o.ID,
		"ip":          o.IP,
		"properties":  o.Properties,
		"session_id":  o.SessionID,
		"updated_ts":  o.UpdatedAt,
		"user_id":     o.UserID,
		"useragent":   o.Useragent,
	}
}

// FromMap attempts to load data into object from a map
func (o *Track) FromMap(kv map[string]interface{}) {

	o.ID = ""

	// if coming from db
	if id, ok := kv["_id"]; ok && id != "" {
		kv["id"] = id
	}

	if val, ok := kv["action"].(string); ok {
		o.Action = val
	} else {
		if val, ok := kv["action"]; ok {
			if val == nil {
				o.Action = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.Action = fmt.Sprintf("%v", val)
			}
		}
	}

	if val, ok := kv["customer_id"].(*string); ok {
		o.CustomerID = val
	} else if val, ok := kv["customer_id"].(string); ok {
		o.CustomerID = &val
	} else {
		if val, ok := kv["customer_id"]; ok {
			if val == nil {
				o.CustomerID = pstrings.Pointer("")
			} else {
				// if coming in as avro union, convert it back
				if kv, ok := val.(map[string]interface{}); ok {
					val = kv["string"]
				}
				o.CustomerID = pstrings.Pointer(fmt.Sprintf("%v", val))
			}
		}
	}

	if val, ok := kv["device_id"].(string); ok {
		o.DeviceID = val
	} else {
		if val, ok := kv["device_id"]; ok {
			if val == nil {
				o.DeviceID = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.DeviceID = fmt.Sprintf("%v", val)
			}
		}
	}

	if val, ok := kv["event"].(string); ok {
		o.Event = val
	} else {
		if val, ok := kv["event"]; ok {
			if val == nil {
				o.Event = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.Event = fmt.Sprintf("%v", val)
			}
		}
	}

	if val, ok := kv["id"].(string); ok {
		o.ID = val
	} else {
		if val, ok := kv["id"]; ok {
			if val == nil {
				o.ID = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.ID = fmt.Sprintf("%v", val)
			}
		}
	}

	if val, ok := kv["ip"].(string); ok {
		o.IP = val
	} else {
		if val, ok := kv["ip"]; ok {
			if val == nil {
				o.IP = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.IP = fmt.Sprintf("%v", val)
			}
		}
	}

	if val, ok := kv["properties"]; ok {
		if val != nil {
			kv := make(map[string]string)
			if m, ok := val.(map[string]string); ok {
				kv = m
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					for k, v := range m {
						if mv, ok := v.(string); ok {
							kv[k] = mv
						} else {
							kv[k] = fmt.Sprintf("%v", v)
						}
					}
				} else {
					panic("unsupported type for properties field entry: " + reflect.TypeOf(val).String())
				}
			}
			o.Properties = kv
		}
	}
	if o.Properties == nil {
		o.Properties = make(map[string]string)
	}

	if val, ok := kv["session_id"].(string); ok {
		o.SessionID = val
	} else {
		if val, ok := kv["session_id"]; ok {
			if val == nil {
				o.SessionID = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.SessionID = fmt.Sprintf("%v", val)
			}
		}
	}

	if val, ok := kv["updated_ts"].(int64); ok {
		o.UpdatedAt = val
	} else {
		if val, ok := kv["updated_ts"]; ok {
			if val == nil {
				o.UpdatedAt = number.ToInt64Any(nil)
			} else {
				if tv, ok := val.(time.Time); ok {
					val = datetime.TimeToEpoch(tv)
				}
				o.UpdatedAt = number.ToInt64Any(val)
			}
		}
	}

	if val, ok := kv["user_id"].(*string); ok {
		o.UserID = val
	} else if val, ok := kv["user_id"].(string); ok {
		o.UserID = &val
	} else {
		if val, ok := kv["user_id"]; ok {
			if val == nil {
				o.UserID = pstrings.Pointer("")
			} else {
				// if coming in as avro union, convert it back
				if kv, ok := val.(map[string]interface{}); ok {
					val = kv["string"]
				}
				o.UserID = pstrings.Pointer(fmt.Sprintf("%v", val))
			}
		}
	}

	if val, ok := kv["useragent"].(string); ok {
		o.Useragent = val
	} else {
		if val, ok := kv["useragent"]; ok {
			if val == nil {
				o.Useragent = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.Useragent = fmt.Sprintf("%v", val)
			}
		}
	}
	o.setDefaults(false)
}

// GetEventAPIConfig returns the EventAPIConfig
func (o *Track) GetEventAPIConfig() datamodel.EventAPIConfig {
	return datamodel.EventAPIConfig{
		Publish: datamodel.EventAPIPublish{
			Public: true,
		},
		Subscribe: datamodel.EventAPISubscribe{
			Public: false,
			Key:    "",
		},
	}
}

type testAction struct {
	wg       sync.WaitGroup
	received datamodel.ModelReceiveEvent
	response datamodel.ModelSendEvent
	err      error
	mu       sync.Mutex
}

var _ Action = (*testAction)(nil)

func (a *testAction) Execute(instance datamodel.ModelReceiveEvent) (datamodel.ModelSendEvent, error) {
	defer a.wg.Done()
	a.mu.Lock()
	a.received = instance
	a.mu.Unlock()
	return a.response, a.err
}

func (a *testAction) New(name datamodel.ModelNameType) datamodel.Model {
	return &Track{}
}

func TestAction(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	action := &testAction{}
	action.wg.Add(1)
	errors := make(chan error, 1)
	config := Config{
		Channel: "dev",
		GroupID: fmt.Sprintf("agenttest-%v", datetime.EpochNow()),
		Factory: action,
		Topic:   TrackTopic.String(),
		Errors:  errors,
		Offset:  "latest",
	}
	sub, err := Register(context.Background(), action, config)
	assert.NoError(err)
	assert.NotNil(sub)
	defer sub.Close()
	time.Sleep(time.Second * 5) // give our listener time to be added
	assert.NoError(event.Publish(context.Background(), event.PublishEvent{
		Object: &Track{
			Event:  "agent",
			Action: "hey",
		},
	}, "dev", ""))
	action.wg.Wait()
	// time.Sleep(time.Second * 60)
	select {
	case err := <-errors:
		assert.NoError(err)
	default:
	}
	assert.NotNil(action.received)
	if m, ok := action.received.Object().(*Track); ok {
		assert.Equal("hey", m.Action)
		assert.Equal("agent", m.Event)
	} else {
		assert.FailNow("should have received *testAction")
	}
}

func TestActionWithResponse(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	action1 := &testAction{
		response: datamodel.NewModelSendEvent(&Track{
			ID:     "456",
			Action: "response",
			Event:  "response",
		}),
	}
	action2 := &testAction{}
	action1.wg.Add(1)
	action2.wg.Add(1)
	errors := make(chan error, 2)
	config1 := Config{
		Channel: "dev",
		GroupID: fmt.Sprintf("agenttest-%v", datetime.EpochNow()),
		Factory: action1,
		Topic:   TrackTopic.String(),
		Errors:  errors,
		Offset:  "latest",
	}
	config2 := Config{
		Channel: "dev",
		GroupID: fmt.Sprintf("agenttest-%v", datetime.EpochNow()),
		Factory: action2,
		Topic:   TrackTopic.String(),
		Errors:  errors,
		Offset:  "latest",
	}
	sub1, err := Register(context.Background(), action1, config1)
	assert.NoError(err)
	assert.NotNil(sub1)
	defer sub1.Close()
	time.Sleep(time.Second * 5) // give our listener time to be added
	assert.NoError(event.Publish(context.Background(), event.PublishEvent{
		Object: &Track{
			ID:     "123",
			Event:  "agent",
			Action: "hey",
		},
	}, "dev", ""))
	action1.wg.Wait()
	sub1.Close()
	select {
	case err := <-errors:
		assert.NoError(err)
	default:
	}
	sub2, err := Register(context.Background(), action2, config2)
	assert.NoError(err)
	assert.NotNil(sub2)
	defer sub2.Close()
	action2.wg.Wait()
	select {
	case err := <-errors:
		assert.NoError(err)
	default:
	}
	action1.mu.Lock()
	action2.mu.Lock()
	defer action1.mu.Unlock()
	defer action2.mu.Unlock()
	assert.NotNil(action1.received)
	if m, ok := action1.received.Object().(*Track); ok {
		assert.Equal("hey", m.Action)
		assert.Equal("agent", m.Event)
	} else {
		assert.FailNow("should have received *testAction")
	}
	if m, ok := action2.received.Object().(*Track); ok {
		assert.Equal("response", m.Action)
		assert.Equal("response", m.Event)
	} else {
		assert.FailNow("should have received *testAction")
	}
}

func TestActionFunc(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	var wg sync.WaitGroup
	wg.Add(1)
	action := NewAction(func(instance datamodel.ModelReceiveEvent) (datamodel.ModelSendEvent, error) {
		defer wg.Done()
		return nil, nil
	})
	factory := &testAction{}
	errors := make(chan error, 1)
	config := Config{
		Channel: "dev",
		GroupID: fmt.Sprintf("agenttest-%v", datetime.EpochNow()),
		Factory: factory,
		Topic:   TrackTopic.String(),
		Errors:  errors,
		Offset:  "latest",
	}
	sub, err := Register(context.Background(), action, config)
	assert.NoError(err)
	assert.NotNil(sub)
	defer sub.Close()
	time.Sleep(time.Second * 5) // give our listener time to be added
	assert.NoError(event.Publish(context.Background(), event.PublishEvent{
		Object: &Track{
			Event:  "agent",
			Action: "hey",
		},
	}, "dev", ""))
	wg.Wait()
	// time.Sleep(time.Second * 60)
	select {
	case err := <-errors:
		assert.NoError(err)
	default:
	}
}
