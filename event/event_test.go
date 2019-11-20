package event

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pinpt/go-common/datamodel"
	"github.com/pinpt/go-common/datetime"
	"github.com/pinpt/go-common/hash"
	pjson "github.com/pinpt/go-common/json"
	"github.com/pinpt/go-common/number"
	pstrings "github.com/pinpt/go-common/strings"
	"github.com/stretchr/testify/assert"
)

const (
	// EchoTopic is the default topic name
	EchoTopic datamodel.TopicNameType = "test_Echo_topic"

	// EchoTable is the default table name
	EchoTable datamodel.ModelNameType = "test_echo"

	// EchoModelName is the model name
	EchoModelName datamodel.ModelNameType = "test.Echo"
)

const (
	// EchoIDColumn is the id column name
	EchoIDColumn = "ID"
	// EchoMessageColumn is the message column name
	EchoMessageColumn = "Message"
	// EchoUpdatedAtColumn is the updated_ts column name
	EchoUpdatedAtColumn = "UpdatedAt"
)

// Echo echo will simplify store data so you can check that it's getting received
type Echo struct {
	// ID some sort of id so you can fetch it back
	ID string `json:"id" codec:"id" bson:"_id" yaml:"id" faker:"-"`
	// Message a message for testing
	Message *string `json:"message,omitempty" codec:"message,omitempty" bson:"message" yaml:"message,omitempty" faker:"-"`
	// UpdatedAt the timestamp that the model was last updated fo real
	UpdatedAt int64 `json:"updated_ts" codec:"updated_ts" bson:"updated_ts" yaml:"updated_ts" faker:"-"`
	// Hashcode stores the hash of the value of this object whereby two objects with the same hashcode are functionality equal
	Hashcode string `json:"hashcode" codec:"hashcode" bson:"hashcode" yaml:"hashcode" faker:"-"`
}

// ensure that this type implements the data model interface
var _ datamodel.Model = (*Echo)(nil)

// ensure that this type implements the streamed data model interface
var _ datamodel.StreamedModel = (*Echo)(nil)

func toEchoObject(o interface{}, isoptional bool) interface{} {
	switch v := o.(type) {
	case *Echo:
		return v.ToMap()

	default:
		return o
	}
}

// String returns a string representation of Echo
func (o *Echo) String() string {
	return fmt.Sprintf("test.Echo<%s>", o.ID)
}

// GetTopicName returns the name of the topic if evented
func (o *Echo) GetTopicName() datamodel.TopicNameType {
	return EchoTopic
}

// GetStreamName returns the name of the stream
func (o *Echo) GetStreamName() string {
	return ""
}

// GetTableName returns the name of the table
func (o *Echo) GetTableName() string {
	return EchoTable.String()
}

// GetModelName returns the name of the model
func (o *Echo) GetModelName() datamodel.ModelNameType {
	return EchoModelName
}

// NewEchoID provides a template for generating an ID field for Echo
func NewEchoID(ID string) string {
	return hash.Values(ID, "name")
}

var emptyString string

func (o *Echo) setDefaults(frommap bool) {
	if o.Message == nil {
		o.Message = &emptyString
	}

	if o.ID == "" {
		o.ID = hash.Values(o.ID, "name")
	}

	if frommap {
		o.FromMap(map[string]interface{}{})
	}
}

// GetID returns the ID for the object
func (o *Echo) GetID() string {
	return o.ID
}

// GetTopicKey returns the topic message key when sending this model as a ModelSendEvent
func (o *Echo) GetTopicKey() string {
	var i interface{} = o.ID
	if s, ok := i.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", i)
}

// GetTimestamp returns the timestamp for the model or now if not provided
func (o *Echo) GetTimestamp() time.Time {
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
	panic("not sure how to handle the date time format for Echo")
}

// IsMaterialized returns true if the model is materialized
func (o *Echo) IsMaterialized() bool {
	return false
}

// GetModelMaterializeConfig returns the materialization config if materialized or nil if not
func (o *Echo) GetModelMaterializeConfig() *datamodel.ModelMaterializeConfig {
	return nil
}

// IsEvented returns true if the model supports eventing and implements ModelEventProvider
func (o *Echo) IsEvented() bool {
	return true
}

// IsMutable returns true if the model is mutable
func (o *Echo) IsMutable() bool {
	return true
}

// SetEventHeaders will set any event headers for the object instance
func (o *Echo) SetEventHeaders(kv map[string]string) {
	kv["model"] = EchoModelName.String()
}

// GetTopicConfig returns the topic config object
func (o *Echo) GetTopicConfig() *datamodel.ModelTopicConfig {
	retention, err := time.ParseDuration("1m0s")
	if err != nil {
		panic("Invalid topic retention duration provided: 1m0s. " + err.Error())
	}

	ttl, err := time.ParseDuration("0s")
	if err != nil {
		ttl = 0
	}
	if ttl == 0 && retention != 0 {
		ttl = retention // they should be the same if not set
	}
	return &datamodel.ModelTopicConfig{
		Key:               "id",
		Timestamp:         "updated_ts",
		NumPartitions:     8,
		CleanupPolicy:     datamodel.CleanupPolicy("delete"),
		ReplicationFactor: 3,
		Retention:         retention,
		MaxSize:           5242880,
		TTL:               ttl,
	}
}

// Clone returns an exact copy of Echo
func (o *Echo) Clone() datamodel.Model {
	c := new(Echo)
	c.FromMap(o.ToMap())
	return c
}

// Anon returns the data structure as anonymous data
func (o *Echo) Anon() datamodel.Model {
	return o
}

// MarshalJSON returns the bytes for marshaling to json
func (o *Echo) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.ToMap())
}

// UnmarshalJSON will unmarshal the json buffer into the object
func (o *Echo) UnmarshalJSON(data []byte) error {
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
func (o *Echo) Stringify() string {
	return pjson.Stringify(o)
}

// IsEqual returns true if the two Echo objects are equal
func (o *Echo) IsEqual(other *Echo) bool {
	return o.GetID() == other.GetID()
}

// ToMap returns the object as a map
func (o *Echo) ToMap() map[string]interface{} {
	o.setDefaults(false)
	return map[string]interface{}{
		"id":         toEchoObject(o.ID, false),
		"message":    toEchoObject(o.Message, true),
		"updated_ts": toEchoObject(o.UpdatedAt, false),
	}
}

// FromMap attempts to load data into object from a map
func (o *Echo) FromMap(kv map[string]interface{}) {

	o.ID = ""

	// if coming from db
	if id, ok := kv["_id"]; ok && id != "" {
		kv["id"] = id
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

	if val, ok := kv["message"].(*string); ok {
		o.Message = val
	} else if val, ok := kv["message"].(string); ok {
		o.Message = &val
	} else {
		if val, ok := kv["message"]; ok {
			if val == nil {
				o.Message = pstrings.Pointer("")
			} else {
				// if coming in as map, convert it back
				if kv, ok := val.(map[string]interface{}); ok {
					val = kv["string"]
				}
				o.Message = pstrings.Pointer(fmt.Sprintf("%v", val))
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
	o.setDefaults(false)
}

// GetEventAPIConfig returns the EventAPIConfig
func (o *Echo) GetEventAPIConfig() datamodel.EventAPIConfig {
	return datamodel.EventAPIConfig{
		Publish: datamodel.EventAPIPublish{
			Public: true,
		},
		Subscribe: datamodel.EventAPISubscribe{
			Public: true,
			Key:    "id",
		},
	}
}

// EchoSendEvent is an event detail for sending data
type EchoSendEvent struct {
	Echo    *Echo
	headers map[string]string
	time    time.Time
	key     string
}

var _ datamodel.ModelSendEvent = (*EchoSendEvent)(nil)

// Key is the key to use for the message
func (e *EchoSendEvent) Key() string {
	if e.key == "" {
		return e.Echo.GetID()
	}
	return e.key
}

// Object returns an instance of the Model that will be send
func (e *EchoSendEvent) Object() datamodel.StreamedModel {
	return e.Echo
}

// Headers returns any headers for the event. can be nil to not send any additional headers
func (e *EchoSendEvent) Headers() map[string]string {
	return e.headers
}

// Timestamp returns the event timestamp. If empty, will default to time.Now()
func (e *EchoSendEvent) Timestamp() time.Time {
	return e.time
}

// EchoSendEventOpts is a function handler for setting opts
type EchoSendEventOpts func(o *EchoSendEvent)

// WithEchoSendEventKey sets the key value to a value different than the object ID
func WithEchoSendEventKey(key string) EchoSendEventOpts {
	return func(o *EchoSendEvent) {
		o.key = key
	}
}

// WithEchoSendEventTimestamp sets the timestamp value
func WithEchoSendEventTimestamp(tv time.Time) EchoSendEventOpts {
	return func(o *EchoSendEvent) {
		o.time = tv
	}
}

// WithEchoSendEventHeader sets the timestamp value
func WithEchoSendEventHeader(key, value string) EchoSendEventOpts {
	return func(o *EchoSendEvent) {
		if o.headers == nil {
			o.headers = make(map[string]string)
		}
		o.headers[key] = value
	}
}

// NewEchoSendEvent returns a new EchoSendEvent instance
func NewEchoSendEvent(o *Echo, opts ...EchoSendEventOpts) *EchoSendEvent {
	res := &EchoSendEvent{
		Echo: o,
	}
	if len(opts) > 0 {
		for _, opt := range opts {
			opt(res)
		}
	}
	return res
}

func TestSendAndReceive(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	errors := make(chan error, 1)
	sub, err := NewSubscription(context.Background(), Subscription{
		GroupID:      fmt.Sprintf("testgroup:%v", datetime.EpochNow()),
		Topics:       []string{EchoTopic.String()},
		IdleDuration: "10s",
		Errors:       errors,
		Channel:      "dev",
		Offset:       "latest",
	})
	assert.NoError(err)
	go func() {
		for err := range errors {
			fmt.Println("ERR", err)
			assert.NoError(err)
			sub.Close()
			break
		}
	}()
	defer sub.Close()
	msg := "Hi"
	echo := &Echo{
		Message: &msg,
	}
	event := PublishEvent{
		Object: echo,
	}
	time.Sleep(time.Second * 5) // let the subscription setup (since we're using latest)
	err = Publish(context.Background(), event, "dev", "")
	assert.NoError(err)
	result := <-sub.Channel()
	assert.NotNil(result)
	kv := make(map[string]interface{})
	assert.NoError(json.Unmarshal([]byte(result.Data), &kv))
	var echo2 Echo
	echo2.FromMap(kv)
	assert.Equal(echo.Stringify(), echo2.Stringify())
}

func TestSendAndReceiveMultipleSync(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	errors := make(chan error, 1)
	sub, err := NewSubscription(context.Background(), Subscription{
		GroupID:      fmt.Sprintf("testgroup:%v", datetime.EpochNow()),
		Topics:       []string{EchoTopic.String()},
		IdleDuration: "10s",
		Errors:       errors,
		Channel:      "dev",
		Offset:       "latest",
	})
	assert.NoError(err)
	go func() {
		for err := range errors {
			fmt.Println("ERR", err)
			assert.NoError(err)
			sub.Close()
			break
		}
	}()
	defer sub.Close()
	time.Sleep(time.Second * 5) // let the subscription setup (since we're using latest)
	for i := 0; i < 100; i++ {
		msg := fmt.Sprintf("%d", i)
		echo := &Echo{
			Message: &msg,
		}
		event := PublishEvent{
			Object: echo,
		}
		err = Publish(context.Background(), event, "dev", "")
		assert.NoError(err)
		result := <-sub.Channel()
		assert.NotNil(result)
		kv := make(map[string]interface{})
		assert.NoError(json.Unmarshal([]byte(result.Data), &kv))
		var echo2 Echo
		echo2.FromMap(kv)
		assert.Equal(echo.Stringify(), echo2.Stringify())
	}
}

func TestSendAndReceiveMultipleAsync(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	errors := make(chan error, 1)
	sub, err := NewSubscription(context.Background(), Subscription{
		GroupID:      fmt.Sprintf("testgroup:%v", datetime.EpochNow()),
		Topics:       []string{EchoTopic.String()},
		IdleDuration: "1m",
		Errors:       errors,
		Channel:      "dev",
		Offset:       "latest",
	})
	assert.NoError(err)
	go func() {
		for err := range errors {
			fmt.Println("ERR", err)
			assert.NoError(err)
			sub.Close()
			break
		}
	}()
	defer sub.Close()
	time.Sleep(time.Second * 5) // let the subscription setup (since we're using latest)
	var wg sync.WaitGroup
	var count int
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range sub.Channel() {
			count++
		}
	}()
	iterations := 100
	for i := 0; i < iterations; i++ {
		msg := fmt.Sprintf("%d", i)
		echo := &Echo{
			Message: &msg,
		}
		event := PublishEvent{
			Object: echo,
		}
		err = Publish(context.Background(), event, "dev", "")
		assert.NoError(err)
	}
	time.Sleep(time.Second) // wait for all the events to come in
	assert.NoError(sub.Close())
	wg.Wait()
	assert.Equal(iterations, count)
}

func TestSendAndReceiveMultipleAsyncWithBuffer(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	errors := make(chan error, 1)
	sub, err := NewSubscription(context.Background(), Subscription{
		GroupID:      fmt.Sprintf("testgroup:%v", datetime.EpochNow()),
		Topics:       []string{EchoTopic.String()},
		IdleDuration: "1m",
		Errors:       errors,
		Channel:      "dev",
		Offset:       "latest",
		BufferSize:   500,
	})
	assert.NoError(err)
	go func() {
		for err := range errors {
			fmt.Println("ERR", err)
			assert.NoError(err)
			sub.Close()
			break
		}
	}()
	defer sub.Close()
	time.Sleep(time.Second * 5) // let the subscription setup (since we're using latest)
	var wg sync.WaitGroup
	var count int
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range sub.Channel() {
			count++
		}
	}()
	iterations := 100
	for i := 0; i < iterations; i++ {
		msg := fmt.Sprintf("%d", i)
		echo := &Echo{
			Message: &msg,
		}
		event := PublishEvent{
			Object: echo,
		}
		err = Publish(context.Background(), event, "dev", "")
		assert.NoError(err)
	}
	time.Sleep(time.Second) // wait for all the events to come in
	assert.NoError(sub.Close())
	wg.Wait()
	assert.Equal(iterations, count)
}

func TestSendAndReceiveMultipleAsyncWithAutocommitDisabled(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.SkipNow()
		return
	}
	assert := assert.New(t)
	errors := make(chan error, 1)
	sub, err := NewSubscription(context.Background(), Subscription{
		GroupID:           fmt.Sprintf("testgroup:%v", datetime.EpochNow()),
		Topics:            []string{EchoTopic.String()},
		IdleDuration:      "1m",
		Errors:            errors,
		Channel:           "dev",
		Offset:            "latest",
		DisableAutoCommit: true,
	})
	assert.NoError(err)
	go func() {
		for err := range errors {
			fmt.Println("ERR", err)
			assert.NoError(err)
			sub.Close()
			break
		}
	}()
	defer sub.Close()
	time.Sleep(time.Second * 5) // let the subscription setup (since we're using latest)
	var wg sync.WaitGroup
	var count int
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range sub.Channel() {
			msg.Commit()
			count++
		}
	}()
	iterations := 100
	for i := 0; i < iterations; i++ {
		msg := fmt.Sprintf("%d", i)
		echo := &Echo{
			Message: &msg,
		}
		event := PublishEvent{
			Object: echo,
		}
		err = Publish(context.Background(), event, "dev", "")
		assert.NoError(err)
	}
	time.Sleep(time.Second) // wait for all the events to come in
	assert.NoError(sub.Close())
	wg.Wait()
	assert.Equal(iterations, count)
	select {
	case err := <-errors:
		assert.NoError(err)
	default:
	}
}

func TestDeadline(t *testing.T) {
	assert := assert.New(t)
	event := PublishEvent{
		Object: &Echo{},
	}
	err := Publish(context.Background(), event, "dev", "", WithDeadline(time.Now()))
	assert.EqualError(err, ErrDeadlineExceeded.Error())
}
