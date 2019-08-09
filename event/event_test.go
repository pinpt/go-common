package event

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/linkedin/goavro"
	"github.com/pinpt/go-common/datamodel"
	"github.com/pinpt/go-common/datetime"
	"github.com/pinpt/go-common/eventing"
	"github.com/pinpt/go-common/fileutil"
	"github.com/pinpt/go-common/hash"
	pjson "github.com/pinpt/go-common/json"
	"github.com/pinpt/go-common/number"
	pstrings "github.com/pinpt/go-common/strings"
	"github.com/stretchr/testify/assert"
)

const (
	// EchoTopic is the default topic name
	EchoTopic datamodel.TopicNameType = "test_Echo_topic"

	// EchoStream is the default stream name
	EchoStream datamodel.TopicNameType = "test_Echo_stream"

	// EchoTable is the default table name
	EchoTable datamodel.TopicNameType = "test_Echo"

	// EchoModelName is the model name
	EchoModelName datamodel.ModelNameType = "test.Echo"
)

const (
	// EchoIDColumn is the id column name
	EchoIDColumn = "id"
	// EchoMessageColumn is the message column name
	EchoMessageColumn = "message"
	// EchoUpdatedAtColumn is the updated_ts column name
	EchoUpdatedAtColumn = "updated_ts"
)

// Echo echo will simplify store data so you can check that it's getting received
type Echo struct {
	// ID some sort of id so you can fetch it back
	ID string `json:"id" bson:"_id" yaml:"id" faker:"-"`
	// Message a message for testing
	Message *string `json:"message" bson:"message" yaml:"message" faker:"-"`
	// UpdatedAt the timestamp that the model was last updated fo real
	UpdatedAt int64 `json:"updated_ts" bson:"updated_ts" yaml:"updated_ts" faker:"-"`
	// Hashcode stores the hash of the value of this object whereby two objects with the same hashcode are functionality equal
	Hashcode string `json:"hashcode" bson:"hashcode" yaml:"hashcode" faker:"-"`
}

// ensure that this type implements the data model interface
var _ datamodel.Model = (*Echo)(nil)

func toEchoObject(o interface{}, isavro bool, isoptional bool, avrotype string) interface{} {
	if res, ok := datamodel.ToGolangObject(o, isavro, isoptional, avrotype); ok {
		return res
	}
	switch v := o.(type) {
	case *Echo:
		return v.ToMap(isavro)

	default:
		panic("couldn't figure out the object type: " + reflect.TypeOf(v).String())
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

// GetModelName returns the name of the model
func (o *Echo) GetModelName() datamodel.ModelNameType {
	return EchoModelName
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
	return &datamodel.ModelTopicConfig{
		Key:               "id",
		Timestamp:         "updated_ts",
		NumPartitions:     8,
		ReplicationFactor: 3,
		Retention:         retention,
		MaxSize:           5242880,
		TTL:               ttl,
	}
}

// GetStateKey returns a key for use in state store
func (o *Echo) GetStateKey() string {
	key := "id"
	return fmt.Sprintf("%s_%s", key, o.GetID())
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

var cachedCodecEcho *goavro.Codec

// GetAvroCodec returns the avro codec for this model
func (o *Echo) GetAvroCodec() *goavro.Codec {
	if cachedCodecEcho == nil {
		c, err := GetEchoAvroSchema()
		if err != nil {
			panic(err)
		}
		cachedCodecEcho = c
	}
	return cachedCodecEcho
}

// ToAvroBinary returns the data as Avro binary data
func (o *Echo) ToAvroBinary() ([]byte, *goavro.Codec, error) {
	kv := o.ToMap(true)
	jbuf, _ := json.Marshal(kv)
	codec := o.GetAvroCodec()
	native, _, err := codec.NativeFromTextual(jbuf)
	if err != nil {
		return nil, nil, err
	}
	// Convert native Go form to binary Avro data
	buf, err := codec.BinaryFromNative(nil, native)
	return buf, codec, err
}

// FromAvroBinary will convert from Avro binary data into data in this object
func (o *Echo) FromAvroBinary(value []byte) error {
	var nullHeader = []byte{byte(0)}
	// if this still has the schema encoded in the header, move past it to the avro payload
	if bytes.HasPrefix(value, nullHeader) {
		value = value[5:]
	}
	kv, _, err := o.GetAvroCodec().NativeFromBinary(value)
	if err != nil {
		return err
	}
	o.FromMap(kv.(map[string]interface{}))
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
func (o *Echo) ToMap(avro ...bool) map[string]interface{} {
	var isavro bool
	if len(avro) > 0 && avro[0] {
		isavro = true
	}
	if isavro {
	}
	o.setDefaults(false)
	return map[string]interface{}{
		"id":         toEchoObject(o.ID, isavro, false, "string"),
		"message":    toEchoObject(o.Message, isavro, true, "string"),
		"updated_ts": toEchoObject(o.UpdatedAt, isavro, false, "long"),
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
				// if coming in as avro union, convert it back
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

// GetEchoAvroSchemaSpec creates the avro schema specification for Echo
func GetEchoAvroSchemaSpec() string {
	spec := map[string]interface{}{
		"type":      "record",
		"namespace": "test",
		"name":      "Echo",
		"fields": []map[string]interface{}{
			map[string]interface{}{
				"name": "id",
				"type": "string",
			},
			map[string]interface{}{
				"name":    "message",
				"type":    []interface{}{"null", "string"},
				"default": nil,
			},
			map[string]interface{}{
				"name": "updated_ts",
				"type": "long",
			},
		},
	}
	return pjson.Stringify(spec, true)
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

// GetEchoAvroSchema creates the avro schema for Echo
func GetEchoAvroSchema() (*goavro.Codec, error) {
	return goavro.NewCodec(GetEchoAvroSchemaSpec())
}

// TransformEchoFunc is a function for transforming Echo during processing
type TransformEchoFunc func(input *Echo) (*Echo, error)

// NewEchoPipe creates a pipe for processing Echo items
func NewEchoPipe(input io.ReadCloser, output io.WriteCloser, errors chan error, transforms ...TransformEchoFunc) <-chan bool {
	done := make(chan bool, 1)
	inch, indone := NewEchoInputStream(input, errors)
	var stream chan Echo
	if len(transforms) > 0 {
		stream = make(chan Echo, 1000)
	} else {
		stream = inch
	}
	outdone := NewEchoOutputStream(output, stream, errors)
	go func() {
		if len(transforms) > 0 {
			var stop bool
			for item := range inch {
				input := &item
				for _, transform := range transforms {
					out, err := transform(input)
					if err != nil {
						stop = true
						errors <- err
						break
					}
					if out == nil {
						input = nil
						break
					} else {
						input = out
					}
				}
				if stop {
					break
				}
				if input != nil {
					stream <- *input
				}
			}
			close(stream)
		}
		<-indone
		<-outdone
		done <- true
	}()
	return done
}

// NewEchoInputStreamDir creates a channel for reading Echo as JSON newlines from a directory of files
func NewEchoInputStreamDir(dir string, errors chan<- error, transforms ...TransformEchoFunc) (chan Echo, <-chan bool) {
	files, err := fileutil.FindFiles(dir, regexp.MustCompile("/test/echo\\.json(\\.gz)?$"))
	if err != nil {
		errors <- err
		ch := make(chan Echo)
		close(ch)
		done := make(chan bool, 1)
		done <- true
		return ch, done
	}
	l := len(files)
	if l > 1 {
		errors <- fmt.Errorf("too many files matched our finder regular expression for echo")
		ch := make(chan Echo)
		close(ch)
		done := make(chan bool, 1)
		done <- true
		return ch, done
	} else if l == 1 {
		return NewEchoInputStreamFile(files[0], errors, transforms...)
	} else {
		ch := make(chan Echo)
		close(ch)
		done := make(chan bool, 1)
		done <- true
		return ch, done
	}
}

// NewEchoInputStreamFile creates an channel for reading Echo as JSON newlines from filename
func NewEchoInputStreamFile(filename string, errors chan<- error, transforms ...TransformEchoFunc) (chan Echo, <-chan bool) {
	of, err := os.Open(filename)
	if err != nil {
		errors <- err
		ch := make(chan Echo)
		close(ch)
		done := make(chan bool, 1)
		done <- true
		return ch, done
	}
	var f io.ReadCloser = of
	if filepath.Ext(filename) == ".gz" {
		gz, err := gzip.NewReader(f)
		if err != nil {
			of.Close()
			errors <- err
			ch := make(chan Echo)
			close(ch)
			done := make(chan bool, 1)
			done <- true
			return ch, done
		}
		f = gz
	}
	return NewEchoInputStream(f, errors, transforms...)
}

// NewEchoInputStream creates an channel for reading Echo as JSON newlines from stream
func NewEchoInputStream(stream io.ReadCloser, errors chan<- error, transforms ...TransformEchoFunc) (chan Echo, <-chan bool) {
	done := make(chan bool, 1)
	ch := make(chan Echo, 1000)
	go func() {
		defer func() { stream.Close(); close(ch); done <- true }()
		r := bufio.NewReader(stream)
		for {
			buf, err := r.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					return
				}
				errors <- err
				return
			}
			var item Echo
			if err := json.Unmarshal(buf, &item); err != nil {
				errors <- err
				return
			}
			in := &item
			var skip bool
			for _, transform := range transforms {
				in, err = transform(in)
				if err != nil {
					errors <- err
					return
				}
				if in == nil {
					skip = true
					break
				}
			}
			if !skip {
				ch <- *in
			}
		}
	}()
	return ch, done
}

// NewEchoOutputStreamDir will output json newlines from channel and save in dir
func NewEchoOutputStreamDir(dir string, ch chan Echo, errors chan<- error, transforms ...TransformEchoFunc) <-chan bool {
	fp := filepath.Join(dir, "/test/echo\\.json(\\.gz)?$")
	os.MkdirAll(filepath.Dir(fp), 0777)
	of, err := os.Create(fp)
	if err != nil {
		errors <- err
		done := make(chan bool, 1)
		done <- true
		return done
	}
	gz, err := gzip.NewWriterLevel(of, gzip.BestCompression)
	if err != nil {
		errors <- err
		done := make(chan bool, 1)
		done <- true
		return done
	}
	return NewEchoOutputStream(gz, ch, errors, transforms...)
}

// NewEchoOutputStream will output json newlines from channel to the stream
func NewEchoOutputStream(stream io.WriteCloser, ch chan Echo, errors chan<- error, transforms ...TransformEchoFunc) <-chan bool {
	done := make(chan bool, 1)
	go func() {
		defer func() {
			if gz, ok := stream.(*gzip.Writer); ok {
				gz.Flush()
				gz.Close()
			}
			stream.Close()
			done <- true
		}()
		for item := range ch {
			in := &item
			var skip bool
			var err error
			for _, transform := range transforms {
				in, err = transform(in)
				if err != nil {
					errors <- err
					return
				}
				if in == nil {
					skip = true
					break
				}
			}
			if !skip {
				buf, err := json.Marshal(in)
				if err != nil {
					errors <- err
					return
				}
				stream.Write(buf)
				stream.Write([]byte{'\n'})
			}
		}
	}()
	return done
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
func (e *EchoSendEvent) Object() datamodel.Model {
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

// NewEchoProducer will stream data from the channel
func NewEchoProducer(ctx context.Context, producer eventing.Producer, ch <-chan datamodel.ModelSendEvent, errors chan<- error, empty chan<- bool) <-chan bool {
	done := make(chan bool, 1)
	go func() {
		defer func() { done <- true }()
		for {
			select {
			case <-ctx.Done():
				return
			case item := <-ch:
				if item == nil {
					empty <- true
					return
				}
				if object, ok := item.Object().(*Echo); ok {
					binary, codec, err := object.ToAvroBinary()
					if err != nil {
						errors <- fmt.Errorf("error encoding %s to avro binary data. %v", object.String(), err)
						return
					}
					headers := map[string]string{}
					object.SetEventHeaders(headers)
					for k, v := range item.Headers() {
						headers[k] = v
					}
					tv := item.Timestamp()
					if tv.IsZero() {
						tv = object.GetTimestamp() // if not provided in the message, use the objects value
					}
					if tv.IsZero() {
						tv = time.Now() // if its still zero, use the ingest time
					}
					msg := eventing.Message{
						Encoding:  eventing.AvroEncoding,
						Key:       item.Key(),
						Value:     binary,
						Codec:     codec,
						Headers:   headers,
						Timestamp: tv,
						Topic:     object.GetTopicName().String(),
					}
					if err := producer.Send(ctx, msg); err != nil {
						errors <- fmt.Errorf("error sending %s. %v", object.String(), err)
					}
				} else {
					errors <- fmt.Errorf("invalid event received. expected an object of type test.Echo but received on of type %v", reflect.TypeOf(item.Object()))
				}
			}
		}
	}()
	return done
}

// NewEchoConsumer will stream data from the topic into the provided channel
func NewEchoConsumer(consumer eventing.Consumer, ch chan<- datamodel.ModelReceiveEvent, errors chan<- error) *eventing.ConsumerCallbackAdapter {
	adapter := &eventing.ConsumerCallbackAdapter{
		OnDataReceived: func(msg eventing.Message) error {
			var object Echo
			switch msg.Encoding {
			case eventing.JSONEncoding:
				if err := json.Unmarshal(msg.Value, &object); err != nil {
					return fmt.Errorf("error unmarshaling json data into test.Echo: %s", err)
				}
			case eventing.AvroEncoding:
				if err := object.FromAvroBinary(msg.Value); err != nil {
					return fmt.Errorf("error unmarshaling avro data into test.Echo: %s", err)
				}
			default:
				return fmt.Errorf("unsure of the encoding since it was not set for test.Echo")
			}

			// ignore messages that have exceeded the TTL
			cfg := object.GetTopicConfig()
			if cfg != nil && cfg.TTL != 0 && msg.Timestamp.UTC().Add(cfg.TTL).Sub(time.Now().UTC()) < 0 {
				return nil
			}
			msg.Codec = object.GetAvroCodec() // match the codec

			ch <- &EchoReceiveEvent{&object, msg, false}
			return nil
		},
		OnErrorReceived: func(err error) {
			errors <- err
		},
		OnEOF: func(topic string, partition int32, offset int64) {
			var object Echo
			var msg eventing.Message
			msg.Topic = topic
			msg.Partition = partition
			msg.Codec = object.GetAvroCodec() // match the codec
			ch <- &EchoReceiveEvent{nil, msg, true}
		},
	}
	consumer.Consume(adapter)
	return adapter
}

// EchoReceiveEvent is an event detail for receiving data
type EchoReceiveEvent struct {
	Echo    *Echo
	message eventing.Message
	eof     bool
}

var _ datamodel.ModelReceiveEvent = (*EchoReceiveEvent)(nil)

// Object returns an instance of the Model that was received
func (e *EchoReceiveEvent) Object() datamodel.Model {
	return e.Echo
}

// Message returns the underlying message data for the event
func (e *EchoReceiveEvent) Message() eventing.Message {
	return e.message
}

// EOF returns true if an EOF event was received. in this case, the Object and Message will return nil
func (e *EchoReceiveEvent) EOF() bool {
	return e.eof
}

// EchoProducer implements the datamodel.ModelEventProducer
type EchoProducer struct {
	ch       chan datamodel.ModelSendEvent
	done     <-chan bool
	producer eventing.Producer
	closed   bool
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	empty    chan bool
}

var _ datamodel.ModelEventProducer = (*EchoProducer)(nil)

// Channel returns the producer channel to produce new events
func (p *EchoProducer) Channel() chan<- datamodel.ModelSendEvent {
	return p.ch
}

// Close is called to shutdown the producer
func (p *EchoProducer) Close() error {
	p.mu.Lock()
	closed := p.closed
	p.closed = true
	p.mu.Unlock()
	if !closed {
		close(p.ch)
		<-p.empty
		p.cancel()
		<-p.done
	}
	return nil
}

// NewProducerChannel returns a channel which can be used for producing Model events
func (o *Echo) NewProducerChannel(producer eventing.Producer, errors chan<- error) datamodel.ModelEventProducer {
	return o.NewProducerChannelSize(producer, 0, errors)
}

// NewProducerChannelSize returns a channel which can be used for producing Model events
func (o *Echo) NewProducerChannelSize(producer eventing.Producer, size int, errors chan<- error) datamodel.ModelEventProducer {
	ch := make(chan datamodel.ModelSendEvent, size)
	empty := make(chan bool, 1)
	newctx, cancel := context.WithCancel(context.Background())
	return &EchoProducer{
		ch:       ch,
		ctx:      newctx,
		cancel:   cancel,
		producer: producer,
		empty:    empty,
		done:     NewEchoProducer(newctx, producer, ch, errors, empty),
	}
}

// NewEchoProducerChannel returns a channel which can be used for producing Model events
func NewEchoProducerChannel(producer eventing.Producer, errors chan<- error) datamodel.ModelEventProducer {
	return NewEchoProducerChannelSize(producer, 0, errors)
}

// NewEchoProducerChannelSize returns a channel which can be used for producing Model events
func NewEchoProducerChannelSize(producer eventing.Producer, size int, errors chan<- error) datamodel.ModelEventProducer {
	ch := make(chan datamodel.ModelSendEvent, size)
	empty := make(chan bool, 1)
	newctx, cancel := context.WithCancel(context.Background())
	return &EchoProducer{
		ch:       ch,
		ctx:      newctx,
		cancel:   cancel,
		producer: producer,
		empty:    empty,
		done:     NewEchoProducer(newctx, producer, ch, errors, empty),
	}
}

// EchoConsumer implements the datamodel.ModelEventConsumer
type EchoConsumer struct {
	ch       chan datamodel.ModelReceiveEvent
	consumer eventing.Consumer
	callback *eventing.ConsumerCallbackAdapter
	closed   bool
	mu       sync.Mutex
}

var _ datamodel.ModelEventConsumer = (*EchoConsumer)(nil)

// Channel returns the consumer channel to consume new events
func (c *EchoConsumer) Channel() <-chan datamodel.ModelReceiveEvent {
	return c.ch
}

// Close is called to shutdown the producer
func (c *EchoConsumer) Close() error {
	c.mu.Lock()
	closed := c.closed
	c.closed = true
	c.mu.Unlock()
	var err error
	if !closed {
		c.callback.Close()
		err = c.consumer.Close()
	}
	return err
}

// NewConsumerChannel returns a consumer channel which can be used to consume Model events
func (o *Echo) NewConsumerChannel(consumer eventing.Consumer, errors chan<- error) datamodel.ModelEventConsumer {
	ch := make(chan datamodel.ModelReceiveEvent)
	return &EchoConsumer{
		ch:       ch,
		callback: NewEchoConsumer(consumer, ch, errors),
		consumer: consumer,
	}
}

// NewEchoConsumerChannel returns a consumer channel which can be used to consume Model events
func NewEchoConsumerChannel(consumer eventing.Consumer, errors chan<- error) datamodel.ModelEventConsumer {
	ch := make(chan datamodel.ModelReceiveEvent)
	return &EchoConsumer{
		ch:       ch,
		callback: NewEchoConsumer(consumer, ch, errors),
		consumer: consumer,
	}
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
