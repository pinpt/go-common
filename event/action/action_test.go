package action

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/pinpt/go-common/event"

	"github.com/linkedin/goavro"
	"github.com/pinpt/go-common/datamodel"
	"github.com/pinpt/go-common/datetime"
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

const (
	// TrackIDColumn is the id column name
	TrackIDColumn = "id"
	// TrackDateAtColumn is the date_ts column name
	TrackDateAtColumn = "date_ts"
	// TrackDateColumn is the date column name
	TrackDateColumn = "date"
	// TrackEventColumn is the event column name
	TrackEventColumn = "event"
	// TrackActionColumn is the action column name
	TrackActionColumn = "action"
	// TrackPropertiesColumn is the properties column name
	TrackPropertiesColumn = "properties"
	// TrackDeviceIDColumn is the device_id column name
	TrackDeviceIDColumn = "device_id"
	// TrackSessionIDColumn is the session_id column name
	TrackSessionIDColumn = "session_id"
	// TrackCustomerIDColumn is the customer_id column name
	TrackCustomerIDColumn = "customer_id"
	// TrackUserIDColumn is the user_id column name
	TrackUserIDColumn = "user_id"
	// TrackIPColumn is the ip column name
	TrackIPColumn = "ip"
	// TrackUseragentColumn is the useragent column name
	TrackUseragentColumn = "useragent"
	// TrackPageColumn is the page column name
	TrackPageColumn = "page"
)

// TrackPage represents the object structure for page
type TrackPage struct {
	// Path the path portion of the page url
	Path string `json:"path" bson:"path" yaml:"path" faker:"-"`
	// Referer the referer portion of the page url
	Referer string `json:"referer" bson:"referer" yaml:"referer" faker:"-"`
	// Title the title portion of the page
	Title string `json:"title" bson:"title" yaml:"title" faker:"-"`
	// URL the url portion of the page
	URL string `json:"url" bson:"url" yaml:"url" faker:"-"`
	// Search the search portion of the page
	Search string `json:"search" bson:"search" yaml:"search" faker:"-"`
	// Language the language portion of the page
	Language string `json:"language" bson:"language" yaml:"language" faker:"-"`
	// Timezone the timezone portion of the page
	Timezone string `json:"timezone" bson:"timezone" yaml:"timezone" faker:"-"`
	// Screen the page screen information
	Screen TrackScreen `json:"screen" bson:"screen" yaml:"screen" faker:"-"`
	// Network the page network information
	Network TrackNetwork `json:"network" bson:"network" yaml:"network" faker:"-"`
}

func (o *TrackPage) ToMap() map[string]interface{} {
	return map[string]interface{}{
		// Path the path portion of the page url
		"path": o.Path,
		// Referer the referer portion of the page url
		"referer": o.Referer,
		// Title the title portion of the page
		"title": o.Title,
		// URL the url portion of the page
		"url": o.URL,
		// Search the search portion of the page
		"search": o.Search,
		// Language the language portion of the page
		"language": o.Language,
		// Timezone the timezone portion of the page
		"timezone": o.Timezone,
		// Screen the page screen information
		"screen": o.Screen,
		// Network the page network information
		"network": o.Network,
	}
}

// TrackScreen represents the object structure for screen
type TrackScreen struct {
	// Density the screen density
	Density int64 `json:"density" bson:"density" yaml:"density" faker:"-"`
	// Width the screen width
	Width int64 `json:"width" bson:"width" yaml:"width" faker:"-"`
	// Height the screen height
	Height int64 `json:"height" bson:"height" yaml:"height" faker:"-"`
}

func (o *TrackScreen) ToMap() map[string]interface{} {
	return map[string]interface{}{
		// Density the screen density
		"density": o.Density,
		// Width the screen width
		"width": o.Width,
		// Height the screen height
		"height": o.Height,
	}
}

// TrackNetwork represents the object structure for network
type TrackNetwork struct {
	// Type the connection type value
	Type string `json:"type" bson:"type" yaml:"type" faker:"-"`
	// Speed the connection speed value
	Speed string `json:"speed" bson:"speed" yaml:"speed" faker:"-"`
}

func (o *TrackNetwork) ToMap() map[string]interface{} {
	return map[string]interface{}{
		// Type the connection type value
		"type": o.Type,
		// Speed the connection speed value
		"speed": o.Speed,
	}
}

// Track track is an event for tracking actions
type Track struct {
	// built in types

	ID string `json:"id" bson:"_id" yaml:"id" faker:"-"`
	// custom types

	// DateAt the date when the track was calculated in UTC format
	DateAt int64 `json:"date_ts" bson:"date_ts" yaml:"date_ts" faker:"-"`
	// Date the date when the track was calculated in RFC3339 format
	Date string `json:"date" bson:"date" yaml:"date" faker:"-"`
	// Event the name of the track event (the noun)
	Event string `json:"event" bson:"event" yaml:"event" faker:"-"`
	// Action the action of the track event (the verb)
	Action string `json:"action" bson:"action" yaml:"action" faker:"-"`
	// Properties the properties of the track event
	Properties map[string]string `json:"properties" bson:"properties" yaml:"properties" faker:"-"`
	// DeviceID the unique device id
	DeviceID string `json:"device_id" bson:"device_id" yaml:"device_id" faker:"-"`
	// SessionID the unique session id
	SessionID string `json:"session_id" bson:"session_id" yaml:"session_id" faker:"-"`
	// CustomerID the customer id for the track event
	CustomerID string `json:"customer_id" bson:"customer_id" yaml:"customer_id" faker:"-"`
	// UserID the user id for the track event
	UserID *string `json:"user_id" bson:"user_id" yaml:"user_id" faker:"-"`
	// IP the ip address for the track event
	IP string `json:"ip" bson:"ip" yaml:"ip" faker:"-"`
	// Useragent the user agent for the track event
	Useragent string `json:"useragent" bson:"useragent" yaml:"useragent" faker:"-"`
	// Page the full page details for the track event
	Page TrackPage `json:"page" bson:"page" yaml:"page" faker:"-"`
}

// ensure that this type implements the data model interface
var _ datamodel.Model = (*Track)(nil)

func toTrackObjectNil(isavro bool, isoptional bool) interface{} {
	if isavro && isoptional {
		return goavro.Union("null", nil)
	}
	return nil
}

func toTrackObject(o interface{}, isavro bool, isoptional bool, avrotype string) interface{} {
	if o == nil {
		return toTrackObjectNil(isavro, isoptional)
	}
	switch v := o.(type) {
	case nil:
		return toTrackObjectNil(isavro, isoptional)
	case string, int, int8, int16, int32, int64, float32, float64, bool:
		if isavro && isoptional {
			return goavro.Union(avrotype, v)
		}
		return v
	case *string:
		if isavro && isoptional {
			if v == nil {
				return toTrackObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv)
		}
		return v
	case *int:
		if isavro && isoptional {
			if v == nil {
				return toTrackObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv)
		}
		return v
	case *int8:
		if isavro && isoptional {
			if v == nil {
				return toTrackObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv)
		}
		return v
	case *int16:
		if isavro && isoptional {
			if v == nil {
				return toTrackObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv)
		}
		return v
	case *int32:
		if isavro && isoptional {
			if v == nil {
				return toTrackObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv)
		}
		return v
	case *int64:
		if isavro && isoptional {
			if v == nil {
				return toTrackObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv)
		}
		return v
	case *float32:
		if isavro && isoptional {
			if v == nil {
				return toTrackObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv)
		}
		return v
	case *float64:
		if isavro && isoptional {
			if v == nil {
				return toTrackObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv)
		}
		return v
	case *bool:
		if isavro && isoptional {
			if v == nil {
				return toTrackObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv)
		}
		return v
	case map[string]interface{}:
		return o
	case *map[string]interface{}:
		return v
	case map[string]string:
		return v
	case *map[string]string:
		return *v
	case *Track:
		return v.ToMap()
	case Track:
		return v.ToMap()
	case []string, []int64, []float64, []bool:
		return o
	case *[]string:
		return (*(o.(*[]string)))
	case *[]int64:
		return (*(o.(*[]int64)))
	case *[]float64:
		return (*(o.(*[]float64)))
	case *[]bool:
		return (*(o.(*[]bool)))
	case []interface{}:
		a := o.([]interface{})
		arr := make([]interface{}, 0)
		for _, av := range a {
			arr = append(arr, toTrackObject(av, isavro, false, ""))
		}
		return arr
	case TrackPage:
		vv := o.(TrackPage)
		return vv.ToMap()
	case *TrackPage:
		return (*o.(*TrackPage)).ToMap()
	case []TrackPage:
		arr := make([]interface{}, 0)
		for _, i := range o.([]TrackPage) {
			arr = append(arr, i.ToMap())
		}
		return arr
	case *[]TrackPage:
		arr := make([]interface{}, 0)
		vv := o.(*[]TrackPage)
		for _, i := range *vv {
			arr = append(arr, i.ToMap())
		}
		return arr
	case TrackScreen:
		vv := o.(TrackScreen)
		return vv.ToMap()
	case *TrackScreen:
		return (*o.(*TrackScreen)).ToMap()
	case []TrackScreen:
		arr := make([]interface{}, 0)
		for _, i := range o.([]TrackScreen) {
			arr = append(arr, i.ToMap())
		}
		return arr
	case *[]TrackScreen:
		arr := make([]interface{}, 0)
		vv := o.(*[]TrackScreen)
		for _, i := range *vv {
			arr = append(arr, i.ToMap())
		}
		return arr
	case TrackNetwork:
		vv := o.(TrackNetwork)
		return vv.ToMap()
	case *TrackNetwork:
		return (*o.(*TrackNetwork)).ToMap()
	case []TrackNetwork:
		arr := make([]interface{}, 0)
		for _, i := range o.([]TrackNetwork) {
			arr = append(arr, i.ToMap())
		}
		return arr
	case *[]TrackNetwork:
		arr := make([]interface{}, 0)
		vv := o.(*[]TrackNetwork)
		for _, i := range *vv {
			arr = append(arr, i.ToMap())
		}
		return arr
	}
	panic("couldn't figure out the object type: " + reflect.TypeOf(o).String())
}

// String returns a string representation of Track
func (o *Track) String() string {
	return fmt.Sprintf("web.Track<%s>", o.ID)
}

// GetTopicName returns the name of the topic if evented
func (o *Track) GetTopicName() datamodel.TopicNameType {
	return TrackTopic
}

// GetModelName returns the name of the model
func (o *Track) GetModelName() datamodel.ModelNameType {
	return TrackModelName
}

func (o *Track) setDefaults() {
	o.GetID()
}

// GetID returns the ID for the object
func (o *Track) GetID() string {
	if o.ID == "" {
		o.ID = hash.Values(o.DateAt, o.DeviceID, o.SessionID, o.Event, "name")
	}
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
	var dt interface{} = o.DateAt
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

// SetEventHeaders will set any event headers for the object instance
func (o *Track) SetEventHeaders(kv map[string]string) {
	kv["model"] = TrackModelName.String()
}

// GetTopicConfig returns the topic config object
func (o *Track) GetTopicConfig() *datamodel.ModelTopicConfig {
	duration, err := time.ParseDuration("168h0m0s")
	if err != nil {
		panic("Invalid topic retention duration provided: 168h0m0s. " + err.Error())
	}
	return &datamodel.ModelTopicConfig{
		Key:               "device_id",
		Timestamp:         "date_ts",
		NumPartitions:     8,
		ReplicationFactor: 3,
		Retention:         duration,
		MaxSize:           5242880,
	}
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
	// make sure that these have values if empty
	o.setDefaults()
	o.FromMap(kv)
	return nil
}

var cachedCodecTrack *goavro.Codec

// GetAvroCodec returns the avro codec for this model
func (o *Track) GetAvroCodec() *goavro.Codec {
	if cachedCodecTrack == nil {
		c, err := GetTrackAvroSchema()
		if err != nil {
			panic(err)
		}
		cachedCodecTrack = c
	}
	return cachedCodecTrack
}

// ToAvroBinary returns the data as Avro binary data
func (o *Track) ToAvroBinary() ([]byte, *goavro.Codec, error) {
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
func (o *Track) FromAvroBinary(value []byte) error {
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
func (o *Track) Stringify() string {
	return pjson.Stringify(o)
}

// IsEqual returns true if the two Track objects are equal
func (o *Track) IsEqual(other *Track) bool {
	return o.GetID() == other.GetID()
}

// ToMap returns the object as a map
func (o *Track) ToMap(avro ...bool) map[string]interface{} {
	var isavro bool
	if len(avro) > 0 && avro[0] {
		isavro = true
	}
	if isavro {
		if o.Properties == nil {
			o.Properties = make(map[string]string)
		}
	}
	return map[string]interface{}{
		"id":          o.GetID(),
		"date_ts":     toTrackObject(o.DateAt, isavro, false, "long"),
		"date":        toTrackObject(o.Date, isavro, false, "string"),
		"event":       toTrackObject(o.Event, isavro, false, "string"),
		"action":      toTrackObject(o.Action, isavro, false, "string"),
		"properties":  toTrackObject(o.Properties, isavro, false, "string"),
		"device_id":   toTrackObject(o.DeviceID, isavro, false, "string"),
		"session_id":  toTrackObject(o.SessionID, isavro, false, "string"),
		"customer_id": toTrackObject(o.CustomerID, isavro, true, "string"),
		"user_id":     toTrackObject(o.UserID, isavro, true, "string"),
		"ip":          toTrackObject(o.IP, isavro, false, "string"),
		"useragent":   toTrackObject(o.Useragent, isavro, false, "string"),
		"page":        toTrackObject(o.Page, isavro, false, "page"),
	}
}

// FromMap attempts to load data into object from a map
func (o *Track) FromMap(kv map[string]interface{}) {
	// make sure that these have values if empty
	o.setDefaults()
	if val, ok := kv["id"].(string); ok {
		o.ID = val
	} else if val, ok := kv["_id"].(string); ok {
		o.ID = val
	}
	if val, ok := kv["date_ts"].(int64); ok {
		o.DateAt = val
	} else {
		val := kv["date_ts"]
		if val == nil {
			o.DateAt = number.ToInt64Any(nil)
		} else {
			o.DateAt = number.ToInt64Any(val)
		}
	}
	if val, ok := kv["date"].(string); ok {
		o.Date = val
	} else {
		val := kv["date"]
		if val == nil {
			o.Date = ""
		} else {
			if m, ok := val.(map[string]interface{}); ok {
				val = pjson.Stringify(m)
			}
			o.Date = fmt.Sprintf("%v", val)
		}
	}
	if val, ok := kv["event"].(string); ok {
		o.Event = val
	} else {
		val := kv["event"]
		if val == nil {
			o.Event = ""
		} else {
			if m, ok := val.(map[string]interface{}); ok {
				val = pjson.Stringify(m)
			}
			o.Event = fmt.Sprintf("%v", val)
		}
	}
	if val, ok := kv["action"].(string); ok {
		o.Action = val
	} else {
		val := kv["action"]
		if val == nil {
			o.Action = ""
		} else {
			if m, ok := val.(map[string]interface{}); ok {
				val = pjson.Stringify(m)
			}
			o.Action = fmt.Sprintf("%v", val)
		}
	}
	if val := kv["properties"]; val != nil {
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
	} else {
		o.Properties = map[string]string{}
	}
	if val, ok := kv["device_id"].(string); ok {
		o.DeviceID = val
	} else {
		val := kv["device_id"]
		if val == nil {
			o.DeviceID = ""
		} else {
			if m, ok := val.(map[string]interface{}); ok {
				val = pjson.Stringify(m)
			}
			o.DeviceID = fmt.Sprintf("%v", val)
		}
	}
	if val, ok := kv["session_id"].(string); ok {
		o.SessionID = val
	} else {
		val := kv["session_id"]
		if val == nil {
			o.SessionID = ""
		} else {
			if m, ok := val.(map[string]interface{}); ok {
				val = pjson.Stringify(m)
			}
			o.SessionID = fmt.Sprintf("%v", val)
		}
	}
	if val, ok := kv["customer_id"].(*string); ok {
		o.CustomerID = *val
	} else if val, ok := kv["customer_id"].(string); ok {
		o.CustomerID = val
	} else {
		val := kv["customer_id"]
		if val == nil {
			o.CustomerID = ""
		} else {
			// if coming in as avro union, convert it back
			if kv, ok := val.(map[string]interface{}); ok {
				val = kv["string"]
			}
			o.CustomerID = fmt.Sprintf("%v", val)
		}
	}
	if val, ok := kv["user_id"].(*string); ok {
		o.UserID = val
	} else if val, ok := kv["user_id"].(string); ok {
		o.UserID = &val
	} else {
		val := kv["user_id"]
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
	if val, ok := kv["ip"].(string); ok {
		o.IP = val
	} else {
		val := kv["ip"]
		if val == nil {
			o.IP = ""
		} else {
			if m, ok := val.(map[string]interface{}); ok {
				val = pjson.Stringify(m)
			}
			o.IP = fmt.Sprintf("%v", val)
		}
	}
	if val, ok := kv["useragent"].(string); ok {
		o.Useragent = val
	} else {
		val := kv["useragent"]
		if val == nil {
			o.Useragent = ""
		} else {
			if m, ok := val.(map[string]interface{}); ok {
				val = pjson.Stringify(m)
			}
			o.Useragent = fmt.Sprintf("%v", val)
		}
	}
	if val, ok := kv["page"].(TrackPage); ok {
		o.Page = val
	} else {
		val := kv["page"]
		if val == nil {
			o.Page = TrackPage{}
		} else {
			o.Page = TrackPage{}
			b, _ := json.Marshal(val)
			json.Unmarshal(b, &o.Page)

		}
	}
}

// GetTrackAvroSchemaSpec creates the avro schema specification for Track
func GetTrackAvroSchemaSpec() string {
	spec := map[string]interface{}{
		"type":         "record",
		"namespace":    "web",
		"name":         "Track",
		"connect.name": "web.Track",
		"fields": []map[string]interface{}{
			map[string]interface{}{
				"name": "id",
				"type": "string",
			},
			map[string]interface{}{
				"name": "date_ts",
				"type": "long",
			},
			map[string]interface{}{
				"name": "date",
				"type": "string",
			},
			map[string]interface{}{
				"name": "event",
				"type": "string",
			},
			map[string]interface{}{
				"name": "action",
				"type": "string",
			},
			map[string]interface{}{
				"name": "properties",
				"type": map[string]interface{}{
					"type":   "map",
					"values": "string",
				},
			},
			map[string]interface{}{
				"name": "device_id",
				"type": "string",
			},
			map[string]interface{}{
				"name": "session_id",
				"type": "string",
			},
			map[string]interface{}{
				"name": "customer_id",
				"type": "string",
			},
			map[string]interface{}{
				"name":    "user_id",
				"type":    []interface{}{"null", "string"},
				"default": nil,
			},
			map[string]interface{}{
				"name": "ip",
				"type": "string",
			},
			map[string]interface{}{
				"name": "useragent",
				"type": "string",
			},
			map[string]interface{}{
				"name": "page",
				"type": map[string]interface{}{"name": "page", "fields": []interface{}{map[string]interface{}{"type": "string", "name": "path", "doc": "the path portion of the page url"}, map[string]interface{}{"type": "string", "name": "referer", "doc": "the referer portion of the page url"}, map[string]interface{}{"type": "string", "name": "title", "doc": "the title portion of the page"}, map[string]interface{}{"type": "string", "name": "url", "doc": "the url portion of the page"}, map[string]interface{}{"type": "string", "name": "search", "doc": "the search portion of the page"}, map[string]interface{}{"type": "string", "name": "language", "doc": "the language portion of the page"}, map[string]interface{}{"doc": "the timezone portion of the page", "type": "string", "name": "timezone"}, map[string]interface{}{"type": map[string]interface{}{"doc": "the page screen information", "type": "record", "name": "page.screen", "fields": []interface{}{map[string]interface{}{"doc": "the screen density", "type": "long", "name": "density"}, map[string]interface{}{"type": "long", "name": "width", "doc": "the screen width"}, map[string]interface{}{"doc": "the screen height", "type": "long", "name": "height"}}}, "name": "screen", "doc": "the page screen information"}, map[string]interface{}{"type": map[string]interface{}{"doc": "the page network information", "type": "record", "name": "page.network", "fields": []interface{}{map[string]interface{}{"type": "string", "name": "type", "doc": "the connection type value"}, map[string]interface{}{"type": "string", "name": "speed", "doc": "the connection speed value"}}}, "name": "network", "doc": "the page network information"}}, "doc": "the full page details for the track event", "type": "record"},
			},
		},
	}
	return pjson.Stringify(spec, true)
}

// GetTrackAvroSchema creates the avro schema for Track
func GetTrackAvroSchema() (*goavro.Codec, error) {
	return goavro.NewCodec(GetTrackAvroSchemaSpec())
}

type testAction struct {
	wg       sync.WaitGroup
	received datamodel.Model
	response datamodel.Model
	err      error
	mu       sync.Mutex
}

var _ Action = (*testAction)(nil)

func (a *testAction) Execute(instance datamodel.Model) (datamodel.Model, error) {
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
	ts := datetime.EpochNow()
	assert.NoError(event.Publish(context.Background(), event.PublishEvent{
		Object: &Track{
			DateAt: ts,
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
	if m, ok := action.received.(*Track); ok {
		assert.Equal("hey", m.Action)
		assert.Equal("agent", m.Event)
		assert.Equal(ts, m.DateAt)
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
		response: &Track{
			ID:     "456",
			Action: "response",
			Event:  "response",
		},
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
	ts := datetime.EpochNow()
	assert.NoError(event.Publish(context.Background(), event.PublishEvent{
		Object: &Track{
			ID:     "123",
			DateAt: ts,
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
	if m, ok := action1.received.(*Track); ok {
		assert.Equal("hey", m.Action)
		assert.Equal("agent", m.Event)
		assert.Equal(ts, m.DateAt)
	} else {
		assert.FailNow("should have received *testAction")
	}
	if m, ok := action2.received.(*Track); ok {
		assert.Equal("response", m.Action)
		assert.Equal("response", m.Event)
	} else {
		assert.FailNow("should have received *testAction")
	}
}
