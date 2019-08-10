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
	// TrackActionColumn is the action column name
	TrackActionColumn = "action"
	// TrackCalculatedDateColumn is the calculated_date column name
	TrackCalculatedDateColumn = "calculated_date"
	// TrackCalculatedDateColumnEpochColumn is the epoch column property of the CalculatedDate name
	TrackCalculatedDateColumnEpochColumn = "calculated_date->epoch"
	// TrackCalculatedDateColumnOffsetColumn is the offset column property of the CalculatedDate name
	TrackCalculatedDateColumnOffsetColumn = "calculated_date->offset"
	// TrackCalculatedDateColumnRfc3339Column is the rfc3339 column property of the CalculatedDate name
	TrackCalculatedDateColumnRfc3339Column = "calculated_date->rfc3339"
	// TrackCustomerIDColumn is the customer_id column name
	TrackCustomerIDColumn = "customer_id"
	// TrackDeviceIDColumn is the device_id column name
	TrackDeviceIDColumn = "device_id"
	// TrackEventColumn is the event column name
	TrackEventColumn = "event"
	// TrackIDColumn is the id column name
	TrackIDColumn = "id"
	// TrackIPColumn is the ip column name
	TrackIPColumn = "ip"
	// TrackPageColumn is the page column name
	TrackPageColumn = "page"
	// TrackPageColumnLanguageColumn is the language column property of the Page name
	TrackPageColumnLanguageColumn = "page->language"
	// TrackPageColumnNetworkColumn is the network column property of the Page name
	TrackPageColumnNetworkColumn = "page->network"
	// TrackPageColumnPathColumn is the path column property of the Page name
	TrackPageColumnPathColumn = "page->path"
	// TrackPageColumnRefererColumn is the referer column property of the Page name
	TrackPageColumnRefererColumn = "page->referer"
	// TrackPageColumnScreenColumn is the screen column property of the Page name
	TrackPageColumnScreenColumn = "page->screen"
	// TrackPageColumnSearchColumn is the search column property of the Page name
	TrackPageColumnSearchColumn = "page->search"
	// TrackPageColumnTimezoneColumn is the timezone column property of the Page name
	TrackPageColumnTimezoneColumn = "page->timezone"
	// TrackPageColumnTitleColumn is the title column property of the Page name
	TrackPageColumnTitleColumn = "page->title"
	// TrackPageColumnURLColumn is the url column property of the Page name
	TrackPageColumnURLColumn = "page->url"
	// TrackPropertiesColumn is the properties column name
	TrackPropertiesColumn = "properties"
	// TrackSessionIDColumn is the session_id column name
	TrackSessionIDColumn = "session_id"
	// TrackUpdatedAtColumn is the updated_ts column name
	TrackUpdatedAtColumn = "updated_ts"
	// TrackUserIDColumn is the user_id column name
	TrackUserIDColumn = "user_id"
	// TrackUseragentColumn is the useragent column name
	TrackUseragentColumn = "useragent"
)

// TrackCalculatedDate represents the object structure for calculated_date
type TrackCalculatedDate struct {
	// Epoch the date in epoch format
	Epoch int64 `json:"epoch" bson:"epoch" yaml:"epoch" faker:"-"`
	// Offset the timezone offset from GMT
	Offset int64 `json:"offset" bson:"offset" yaml:"offset" faker:"-"`
	// Rfc3339 the date in RFC3339 format
	Rfc3339 string `json:"rfc3339" bson:"rfc3339" yaml:"rfc3339" faker:"-"`
}

func toTrackCalculatedDateObjectNil(isavro bool, isoptional bool) interface{} {
	if isavro && isoptional {
		return goavro.Union("null", nil)
	}
	return nil
}

func toTrackCalculatedDateObject(o interface{}, isavro bool, isoptional bool, avrotype string) interface{} {
	if res, ok := datamodel.ToGolangObject(o, isavro, isoptional, avrotype); ok {
		return res
	}
	switch v := o.(type) {
	case *TrackCalculatedDate:
		return v.ToMap(isavro)

	default:
		panic("couldn't figure out the object type: " + reflect.TypeOf(v).String())
	}
}

func (o *TrackCalculatedDate) ToMap(avro ...bool) map[string]interface{} {
	var isavro bool
	if len(avro) > 0 && avro[0] {
		isavro = true
	}
	o.setDefaults(true)
	return map[string]interface{}{
		// Epoch the date in epoch format
		"epoch": toTrackCalculatedDateObject(o.Epoch, isavro, false, "long"),
		// Offset the timezone offset from GMT
		"offset": toTrackCalculatedDateObject(o.Offset, isavro, false, "long"),
		// Rfc3339 the date in RFC3339 format
		"rfc3339": toTrackCalculatedDateObject(o.Rfc3339, isavro, false, "string"),
	}
}

func (o *TrackCalculatedDate) setDefaults(frommap bool) {

	if frommap {
		o.FromMap(map[string]interface{}{})
	}
}

// FromMap attempts to load data into object from a map
func (o *TrackCalculatedDate) FromMap(kv map[string]interface{}) {

	if val, ok := kv["epoch"].(int64); ok {
		o.Epoch = val
	} else {
		if val, ok := kv["epoch"]; ok {
			if val == nil {
				o.Epoch = number.ToInt64Any(nil)
			} else {
				if tv, ok := val.(time.Time); ok {
					val = datetime.TimeToEpoch(tv)
				}
				o.Epoch = number.ToInt64Any(val)
			}
		}
	}

	if val, ok := kv["offset"].(int64); ok {
		o.Offset = val
	} else {
		if val, ok := kv["offset"]; ok {
			if val == nil {
				o.Offset = number.ToInt64Any(nil)
			} else {
				if tv, ok := val.(time.Time); ok {
					val = datetime.TimeToEpoch(tv)
				}
				o.Offset = number.ToInt64Any(val)
			}
		}
	}

	if val, ok := kv["rfc3339"].(string); ok {
		o.Rfc3339 = val
	} else {
		if val, ok := kv["rfc3339"]; ok {
			if val == nil {
				o.Rfc3339 = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.Rfc3339 = fmt.Sprintf("%v", val)
			}
		}
	}
	o.setDefaults(false)
}

// TrackPageNetwork represents the object structure for network
type TrackPageNetwork struct {
	// Type the connection type value
	Type string `json:"type" bson:"type" yaml:"type" faker:"-"`
	// Speed the connection speed value
	Speed string `json:"speed" bson:"speed" yaml:"speed" faker:"-"`
}

func toTrackPageNetworkObjectNil(isavro bool, isoptional bool) interface{} {
	if isavro && isoptional {
		return goavro.Union("null", nil)
	}
	return nil
}

func toTrackPageNetworkObject(o interface{}, isavro bool, isoptional bool, avrotype string) interface{} {
	if res, ok := datamodel.ToGolangObject(o, isavro, isoptional, avrotype); ok {
		return res
	}
	switch v := o.(type) {
	case *TrackPageNetwork:
		return v.ToMap(isavro)

	default:
		panic("couldn't figure out the object type: " + reflect.TypeOf(v).String())
	}
}

func (o *TrackPageNetwork) ToMap(avro ...bool) map[string]interface{} {
	var isavro bool
	if len(avro) > 0 && avro[0] {
		isavro = true
	}
	o.setDefaults(true)
	return map[string]interface{}{
		// Type the connection type value
		"type": toTrackPageNetworkObject(o.Type, isavro, false, "string"),
		// Speed the connection speed value
		"speed": toTrackPageNetworkObject(o.Speed, isavro, false, "string"),
	}
}

func (o *TrackPageNetwork) setDefaults(frommap bool) {

	if frommap {
		o.FromMap(map[string]interface{}{})
	}
}

// FromMap attempts to load data into object from a map
func (o *TrackPageNetwork) FromMap(kv map[string]interface{}) {

	if val, ok := kv["type"].(string); ok {
		o.Type = val
	} else {
		if val, ok := kv["type"]; ok {
			if val == nil {
				o.Type = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.Type = fmt.Sprintf("%v", val)
			}
		}
	}

	if val, ok := kv["speed"].(string); ok {
		o.Speed = val
	} else {
		if val, ok := kv["speed"]; ok {
			if val == nil {
				o.Speed = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.Speed = fmt.Sprintf("%v", val)
			}
		}
	}
	o.setDefaults(false)
}

// TrackPageScreen represents the object structure for screen
type TrackPageScreen struct {
	// Density the screen density
	Density int64 `json:"density" bson:"density" yaml:"density" faker:"-"`
	// Width the screen width
	Width int64 `json:"width" bson:"width" yaml:"width" faker:"-"`
	// Height the screen height
	Height int64 `json:"height" bson:"height" yaml:"height" faker:"-"`
}

func toTrackPageScreenObjectNil(isavro bool, isoptional bool) interface{} {
	if isavro && isoptional {
		return goavro.Union("null", nil)
	}
	return nil
}

func toTrackPageScreenObject(o interface{}, isavro bool, isoptional bool, avrotype string) interface{} {
	if res, ok := datamodel.ToGolangObject(o, isavro, isoptional, avrotype); ok {
		return res
	}
	switch v := o.(type) {
	case *TrackPageScreen:
		return v.ToMap(isavro)

	default:
		panic("couldn't figure out the object type: " + reflect.TypeOf(v).String())
	}
}

func (o *TrackPageScreen) ToMap(avro ...bool) map[string]interface{} {
	var isavro bool
	if len(avro) > 0 && avro[0] {
		isavro = true
	}
	o.setDefaults(true)
	return map[string]interface{}{
		// Density the screen density
		"density": toTrackPageScreenObject(o.Density, isavro, false, "long"),
		// Width the screen width
		"width": toTrackPageScreenObject(o.Width, isavro, false, "long"),
		// Height the screen height
		"height": toTrackPageScreenObject(o.Height, isavro, false, "long"),
	}
}

func (o *TrackPageScreen) setDefaults(frommap bool) {

	if frommap {
		o.FromMap(map[string]interface{}{})
	}
}

// FromMap attempts to load data into object from a map
func (o *TrackPageScreen) FromMap(kv map[string]interface{}) {

	if val, ok := kv["density"].(int64); ok {
		o.Density = val
	} else {
		if val, ok := kv["density"]; ok {
			if val == nil {
				o.Density = number.ToInt64Any(nil)
			} else {
				if tv, ok := val.(time.Time); ok {
					val = datetime.TimeToEpoch(tv)
				}
				o.Density = number.ToInt64Any(val)
			}
		}
	}

	if val, ok := kv["width"].(int64); ok {
		o.Width = val
	} else {
		if val, ok := kv["width"]; ok {
			if val == nil {
				o.Width = number.ToInt64Any(nil)
			} else {
				if tv, ok := val.(time.Time); ok {
					val = datetime.TimeToEpoch(tv)
				}
				o.Width = number.ToInt64Any(val)
			}
		}
	}

	if val, ok := kv["height"].(int64); ok {
		o.Height = val
	} else {
		if val, ok := kv["height"]; ok {
			if val == nil {
				o.Height = number.ToInt64Any(nil)
			} else {
				if tv, ok := val.(time.Time); ok {
					val = datetime.TimeToEpoch(tv)
				}
				o.Height = number.ToInt64Any(val)
			}
		}
	}
	o.setDefaults(false)
}

// TrackPage represents the object structure for page
type TrackPage struct {
	// Language the language portion of the page
	Language string `json:"language" bson:"language" yaml:"language" faker:"-"`
	// Network the page network information
	Network TrackPageNetwork `json:"network" bson:"network" yaml:"network" faker:"-"`
	// Path the path portion of the page url
	Path string `json:"path" bson:"path" yaml:"path" faker:"-"`
	// Referer the referer portion of the page url
	Referer string `json:"referer" bson:"referer" yaml:"referer" faker:"-"`
	// Screen the page screen information
	Screen TrackPageScreen `json:"screen" bson:"screen" yaml:"screen" faker:"-"`
	// Search the search portion of the page
	Search string `json:"search" bson:"search" yaml:"search" faker:"-"`
	// Timezone the timezone portion of the page
	Timezone string `json:"timezone" bson:"timezone" yaml:"timezone" faker:"-"`
	// Title the title portion of the page
	Title string `json:"title" bson:"title" yaml:"title" faker:"-"`
	// URL the url portion of the page
	URL string `json:"url" bson:"url" yaml:"url" faker:"-"`
}

func toTrackPageObjectNil(isavro bool, isoptional bool) interface{} {
	if isavro && isoptional {
		return goavro.Union("null", nil)
	}
	return nil
}

func toTrackPageObject(o interface{}, isavro bool, isoptional bool, avrotype string) interface{} {
	if res, ok := datamodel.ToGolangObject(o, isavro, isoptional, avrotype); ok {
		return res
	}
	switch v := o.(type) {
	case *TrackPage:
		return v.ToMap(isavro)

	case TrackPageNetwork:
		return v.ToMap(isavro)

	case TrackPageScreen:
		return v.ToMap(isavro)

	default:
		panic("couldn't figure out the object type: " + reflect.TypeOf(v).String())
	}
}

func (o *TrackPage) ToMap(avro ...bool) map[string]interface{} {
	var isavro bool
	if len(avro) > 0 && avro[0] {
		isavro = true
	}
	o.setDefaults(true)
	return map[string]interface{}{
		// Language the language portion of the page
		"language": toTrackPageObject(o.Language, isavro, false, "string"),
		// Network the page network information
		"network": toTrackPageObject(o.Network, isavro, false, "network"),
		// Path the path portion of the page url
		"path": toTrackPageObject(o.Path, isavro, false, "string"),
		// Referer the referer portion of the page url
		"referer": toTrackPageObject(o.Referer, isavro, false, "string"),
		// Screen the page screen information
		"screen": toTrackPageObject(o.Screen, isavro, false, "screen"),
		// Search the search portion of the page
		"search": toTrackPageObject(o.Search, isavro, false, "string"),
		// Timezone the timezone portion of the page
		"timezone": toTrackPageObject(o.Timezone, isavro, false, "string"),
		// Title the title portion of the page
		"title": toTrackPageObject(o.Title, isavro, false, "string"),
		// URL the url portion of the page
		"url": toTrackPageObject(o.URL, isavro, false, "string"),
	}
}

func (o *TrackPage) setDefaults(frommap bool) {

	if frommap {
		o.FromMap(map[string]interface{}{})
	}
}

// FromMap attempts to load data into object from a map
func (o *TrackPage) FromMap(kv map[string]interface{}) {

	if val, ok := kv["language"].(string); ok {
		o.Language = val
	} else {
		if val, ok := kv["language"]; ok {
			if val == nil {
				o.Language = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.Language = fmt.Sprintf("%v", val)
			}
		}
	}

	if val, ok := kv["network"]; ok {
		if kv, ok := val.(map[string]interface{}); ok {
			o.Network.FromMap(kv)
		} else if sv, ok := val.(TrackPageNetwork); ok {
			// struct
			o.Network = sv
		} else if sp, ok := val.(*TrackPageNetwork); ok {
			// struct pointer
			o.Network = *sp
		}
	} else {
		o.Network.FromMap(map[string]interface{}{})
	}

	if val, ok := kv["path"].(string); ok {
		o.Path = val
	} else {
		if val, ok := kv["path"]; ok {
			if val == nil {
				o.Path = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.Path = fmt.Sprintf("%v", val)
			}
		}
	}

	if val, ok := kv["referer"].(string); ok {
		o.Referer = val
	} else {
		if val, ok := kv["referer"]; ok {
			if val == nil {
				o.Referer = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.Referer = fmt.Sprintf("%v", val)
			}
		}
	}

	if val, ok := kv["screen"]; ok {
		if kv, ok := val.(map[string]interface{}); ok {
			o.Screen.FromMap(kv)
		} else if sv, ok := val.(TrackPageScreen); ok {
			// struct
			o.Screen = sv
		} else if sp, ok := val.(*TrackPageScreen); ok {
			// struct pointer
			o.Screen = *sp
		}
	} else {
		o.Screen.FromMap(map[string]interface{}{})
	}

	if val, ok := kv["search"].(string); ok {
		o.Search = val
	} else {
		if val, ok := kv["search"]; ok {
			if val == nil {
				o.Search = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.Search = fmt.Sprintf("%v", val)
			}
		}
	}

	if val, ok := kv["timezone"].(string); ok {
		o.Timezone = val
	} else {
		if val, ok := kv["timezone"]; ok {
			if val == nil {
				o.Timezone = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.Timezone = fmt.Sprintf("%v", val)
			}
		}
	}

	if val, ok := kv["title"].(string); ok {
		o.Title = val
	} else {
		if val, ok := kv["title"]; ok {
			if val == nil {
				o.Title = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.Title = fmt.Sprintf("%v", val)
			}
		}
	}

	if val, ok := kv["url"].(string); ok {
		o.URL = val
	} else {
		if val, ok := kv["url"]; ok {
			if val == nil {
				o.URL = ""
			} else {
				if m, ok := val.(map[string]interface{}); ok {
					val = pjson.Stringify(m)
				}
				o.URL = fmt.Sprintf("%v", val)
			}
		}
	}
	o.setDefaults(false)
}

// Track track is an event for tracking actions
type Track struct {
	// Action the action of the track event (the verb)
	Action string `json:"action" bson:"action" yaml:"action" faker:"-"`
	// CalculatedDate the date when the track was calculated
	CalculatedDate TrackCalculatedDate `json:"calculated_date" bson:"calculated_date" yaml:"calculated_date" faker:"-"`
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
	Page TrackPage `json:"page" bson:"page" yaml:"page" faker:"-"`
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

func toTrackObjectNil(isavro bool, isoptional bool) interface{} {
	if isavro && isoptional {
		return goavro.Union("null", nil)
	}
	return nil
}

func toTrackObject(o interface{}, isavro bool, isoptional bool, avrotype string) interface{} {
	if res, ok := datamodel.ToGolangObject(o, isavro, isoptional, avrotype); ok {
		return res
	}
	switch v := o.(type) {
	case *Track:
		return v.ToMap(isavro)

	case TrackCalculatedDate:
		return v.ToMap(isavro)

	case TrackPage:
		return v.ToMap(isavro)

	default:
		panic("couldn't figure out the object type: " + reflect.TypeOf(v).String())
	}
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

var emptyString string

func (o *Track) setDefaults(frommap bool) {
	if o.CustomerID == nil {
		o.CustomerID = &emptyString
	}
	if o.UserID == nil {
		o.UserID = &emptyString
	}

	if o.ID == "" {
		o.ID = hash.Values(o.CalculatedDate.Epoch, o.DeviceID, o.SessionID, o.Event, o.Action)
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

// GetStateKey returns a key for use in state store
func (o *Track) GetStateKey() string {
	key := "device_id"
	return fmt.Sprintf("%s_%s", key, o.GetID())
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
	o.setDefaults(false)
	return map[string]interface{}{
		"action":          toTrackObject(o.Action, isavro, false, "string"),
		"calculated_date": toTrackObject(o.CalculatedDate, isavro, false, "calculated_date"),
		"customer_id":     toTrackObject(o.CustomerID, isavro, true, "string"),
		"device_id":       toTrackObject(o.DeviceID, isavro, false, "string"),
		"event":           toTrackObject(o.Event, isavro, false, "string"),
		"id":              toTrackObject(o.ID, isavro, false, "string"),
		"ip":              toTrackObject(o.IP, isavro, false, "string"),
		"page":            toTrackObject(o.Page, isavro, false, "page"),
		"properties":      toTrackObject(o.Properties, isavro, false, "string"),
		"session_id":      toTrackObject(o.SessionID, isavro, false, "string"),
		"updated_ts":      toTrackObject(o.UpdatedAt, isavro, false, "long"),
		"user_id":         toTrackObject(o.UserID, isavro, true, "string"),
		"useragent":       toTrackObject(o.Useragent, isavro, false, "string"),
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

	if val, ok := kv["calculated_date"]; ok {
		if kv, ok := val.(map[string]interface{}); ok {
			o.CalculatedDate.FromMap(kv)
		} else if sv, ok := val.(TrackCalculatedDate); ok {
			// struct
			o.CalculatedDate = sv
		} else if sp, ok := val.(*TrackCalculatedDate); ok {
			// struct pointer
			o.CalculatedDate = *sp
		} else if dt, ok := val.(*datetime.Date); ok && dt != nil {
			o.CalculatedDate.Epoch = dt.Epoch
			o.CalculatedDate.Rfc3339 = dt.Rfc3339
			o.CalculatedDate.Offset = dt.Offset
		} else if tv, ok := val.(time.Time); ok && !tv.IsZero() {
			dt, err := datetime.NewDateWithTime(tv)
			if err != nil {
				panic(err)
			}
			o.CalculatedDate.Epoch = dt.Epoch
			o.CalculatedDate.Rfc3339 = dt.Rfc3339
			o.CalculatedDate.Offset = dt.Offset
		} else if s, ok := val.(string); ok && s != "" {
			dt, err := datetime.NewDate(s)
			if err == nil {
				o.CalculatedDate.Epoch = dt.Epoch
				o.CalculatedDate.Rfc3339 = dt.Rfc3339
				o.CalculatedDate.Offset = dt.Offset
			}
		}
	} else {
		o.CalculatedDate.FromMap(map[string]interface{}{})
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

	if val, ok := kv["page"]; ok {
		if kv, ok := val.(map[string]interface{}); ok {
			o.Page.FromMap(kv)
		} else if sv, ok := val.(TrackPage); ok {
			// struct
			o.Page = sv
		} else if sp, ok := val.(*TrackPage); ok {
			// struct pointer
			o.Page = *sp
		}
	} else {
		o.Page.FromMap(map[string]interface{}{})
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

// GetTrackAvroSchemaSpec creates the avro schema specification for Track
func GetTrackAvroSchemaSpec() string {
	spec := map[string]interface{}{
		"type":      "record",
		"namespace": "web",
		"name":      "Track",
		"fields": []map[string]interface{}{
			map[string]interface{}{
				"name": "action",
				"type": "string",
			},
			map[string]interface{}{
				"name": "calculated_date",
				"type": map[string]interface{}{"doc": "the date when the track was calculated", "fields": []interface{}{map[string]interface{}{"doc": "the date in epoch format", "name": "epoch", "type": "long"}, map[string]interface{}{"doc": "the timezone offset from GMT", "name": "offset", "type": "long"}, map[string]interface{}{"doc": "the date in RFC3339 format", "name": "rfc3339", "type": "string"}}, "name": "calculated_date", "type": "record"},
			},
			map[string]interface{}{
				"name":    "customer_id",
				"type":    []interface{}{"null", "string"},
				"default": nil,
			},
			map[string]interface{}{
				"name": "device_id",
				"type": "string",
			},
			map[string]interface{}{
				"name": "event",
				"type": "string",
			},
			map[string]interface{}{
				"name": "id",
				"type": "string",
			},
			map[string]interface{}{
				"name": "ip",
				"type": "string",
			},
			map[string]interface{}{
				"name": "page",
				"type": map[string]interface{}{"doc": "the full page details for the track event", "fields": []interface{}{map[string]interface{}{"doc": "the language portion of the page", "name": "language", "type": "string"}, map[string]interface{}{"doc": "the page network information", "name": "network", "type": map[string]interface{}{"doc": "the page network information", "fields": []interface{}{map[string]interface{}{"doc": "the connection type value", "name": "type", "type": "string"}, map[string]interface{}{"doc": "the connection speed value", "name": "speed", "type": "string"}}, "name": "page.network", "type": "record"}}, map[string]interface{}{"doc": "the path portion of the page url", "name": "path", "type": "string"}, map[string]interface{}{"doc": "the referer portion of the page url", "name": "referer", "type": "string"}, map[string]interface{}{"doc": "the page screen information", "name": "screen", "type": map[string]interface{}{"doc": "the page screen information", "fields": []interface{}{map[string]interface{}{"doc": "the screen density", "name": "density", "type": "long"}, map[string]interface{}{"doc": "the screen width", "name": "width", "type": "long"}, map[string]interface{}{"doc": "the screen height", "name": "height", "type": "long"}}, "name": "page.screen", "type": "record"}}, map[string]interface{}{"doc": "the search portion of the page", "name": "search", "type": "string"}, map[string]interface{}{"doc": "the timezone portion of the page", "name": "timezone", "type": "string"}, map[string]interface{}{"doc": "the title portion of the page", "name": "title", "type": "string"}, map[string]interface{}{"doc": "the url portion of the page", "name": "url", "type": "string"}}, "name": "page", "type": "record"},
			},
			map[string]interface{}{
				"name": "properties",
				"type": map[string]interface{}{
					"type":   "map",
					"values": "string",
				},
			},
			map[string]interface{}{
				"name": "session_id",
				"type": "string",
			},
			map[string]interface{}{
				"name": "updated_ts",
				"type": "long",
			},
			map[string]interface{}{
				"name":    "user_id",
				"type":    []interface{}{"null", "string"},
				"default": nil,
			},
			map[string]interface{}{
				"name": "useragent",
				"type": "string",
			},
		},
	}
	return pjson.Stringify(spec, true)
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

// GetTrackAvroSchema creates the avro schema for Track
func GetTrackAvroSchema() (*goavro.Codec, error) {
	return goavro.NewCodec(GetTrackAvroSchemaSpec())
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
	ts := datetime.NewDateNow()
	assert.NoError(event.Publish(context.Background(), event.PublishEvent{
		Object: &Track{
			CalculatedDate: TrackCalculatedDate{ts.Epoch, ts.Offset, ts.Rfc3339},
			Event:          "agent",
			Action:         "hey",
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
		assert.Equal(ts.Epoch, m.CalculatedDate.Epoch)
		assert.Equal(ts.Offset, m.CalculatedDate.Offset)
		assert.Equal(ts.Rfc3339, m.CalculatedDate.Rfc3339)
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
	ts := datetime.NewDateNow()
	assert.NoError(event.Publish(context.Background(), event.PublishEvent{
		Object: &Track{
			ID:             "123",
			CalculatedDate: TrackCalculatedDate{ts.Epoch, ts.Offset, ts.Rfc3339},
			Event:          "agent",
			Action:         "hey",
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
		assert.Equal(ts.Epoch, m.CalculatedDate.Epoch)
		assert.Equal(ts.Offset, m.CalculatedDate.Offset)
		assert.Equal(ts.Rfc3339, m.CalculatedDate.Rfc3339)
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
	ts := datetime.NewDateNow()
	assert.NoError(event.Publish(context.Background(), event.PublishEvent{
		Object: &Track{
			CalculatedDate: TrackCalculatedDate{ts.Epoch, ts.Offset, ts.Rfc3339},
			Event:          "agent",
			Action:         "hey",
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
