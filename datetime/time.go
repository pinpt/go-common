package datetime

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/jhaynie/go-gator/orm"
)

const (
	SignalTimeUnit_NOW          int32 = 0
	SignalTimeUnit_MONTH        int32 = 30
	SignalTimeUnit_QUARTER      int32 = 90
	SignalTimeUnit_HALFYEAR     int32 = 180
	SignalTimeUnit_YEAR         int32 = 365
	SignalTimeUnit_THIRDQUARTER int32 = 270
	SignalTimeUnit_ALLTIME      int32 = -1
	SignalTimeUnit_BIMONTH      int32 = 60
)

func GetTimeUnitString(timeUnit int32) string {
	switch timeUnit {
	case SignalTimeUnit_NOW:
		{
			return "now"
		}
	case SignalTimeUnit_MONTH:
		{
			return "month"
		}
	case SignalTimeUnit_QUARTER:
		{
			return "quarter"
		}
	case SignalTimeUnit_BIMONTH:
		{
			return "bimonth"
		}
	case SignalTimeUnit_HALFYEAR:
		{
			return "halfyear"
		}
	case SignalTimeUnit_THIRDQUARTER:
		{
			return "thirdquarter"
		}
	case SignalTimeUnit_YEAR:
		{
			return "year"
		}
	}
	return "alltime"
}

// ISODate returns a RFC 3339 formatted string for the current date time
func ISODate() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// ISODateFromTime returns a RFC 3339 formatted string from the supplied timestamp
func ISODateFromTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// ISODateToTime returns a RFC 3339 formatted string as a timestamp
func ISODateToTime(date string) (time.Time, error) {
	if strings.HasSuffix(date, "Z") {
		return time.Parse("2006-01-02T15:04:05Z", date)
	}
	return ISODateOffsetToTime(date)
}

// ISODateOffsetToTime returns a RFC 3339 formatted string as a timestamp
func ISODateOffsetToTime(date string) (time.Time, error) {
	if date == "" {
		return time.Time{}, nil
	}
	if strings.Contains(date, "Z") {
		// 2017-01-20T15:56:23.000000Z-08:00
		tv, err := time.Parse("2006-01-02T15:04:05.999999999Z-07:00", date)
		if err == nil {
			return tv, nil
		}
	}
	if strings.Contains(date, ".") {
		tv, err := time.Parse("2006-01-02T15:04:05.999999999-07:00", date)
		if err != nil {
			tv, err = time.Parse("2006-01-02T15:04:05.999999999-0700", date)
			if err != nil {
				return time.Parse("2006-01-02T15:04:05.000000-07:00", date)
			}
		}
		return tv, nil
	}

	match, _ := regexp.MatchString("([+-]\\d{2}:\\d{2})", date)
	if match {
		return time.Parse("2006-01-02T15:04:05-07:00", date)
	}

	return time.Parse("2006-01-02T15:04:05-0700", date)
}

// ISODateToEpoch returns an epoch date or 0 if invalid or empty
func ISODateToEpoch(date string) (int64, error) {
	if date == "" {
		return 0, nil
	}
	ts, err := ISODateToTime(date)
	if err != nil {
		return 0, err
	}
	return TimeToEpoch(ts), nil
}

// TimestampToEpoch will convert a timestamp to epoch (in UTC) with millisecond precision
func TimestampToEpoch(ts *timestamp.Timestamp) int64 {
	tv, err := ptypes.Timestamp(ts)
	if err == nil {
		return tv.UTC().UnixNano() / 1000000
	}
	return 0
}

// TimeToEpoch will convert a time to epoch (in UTC) with millisecond precision
func TimeToEpoch(tv time.Time) int64 {
	if tv.IsZero() {
		return 0
	}
	return tv.UTC().UnixNano() / 1000000
}

// EpochNow will return the current time in epoch (in UTC) with millisecond precision
func EpochNow() int64 {
	return time.Now().UTC().UnixNano() / 1000000
}

// ISODateFromTimestamp returns a a RFC 3339 formatted string from a protobuf timestamp
func ISODateFromTimestamp(t *timestamp.Timestamp) string {
	tv, err := ptypes.Timestamp(t)
	if err == nil {
		return ISODateFromTime(tv)
	}
	return fmt.Sprintf("<invalid date:%v:%v>", t, err)
}

// ISODateFromSQLNullTime returns a a RFC 3339 formatted string from a mysql.NullTime
func ISODateFromSQLNullTime(t mysql.NullTime) string {
	return ISODateFromTime(t.Time)
}

// ShortDateFromTimestamp returns a DATE (no time) formatted string from a protobuf timestamp
func ShortDateFromTimestamp(t *timestamp.Timestamp) string {
	if t == nil {
		return ""
	}
	tv, err := ptypes.Timestamp(t)
	if err == nil {
		return tv.UTC().Format("2006-01-02")
	}
	return fmt.Sprintf("<invalid date:%v:%v>", t, err)
}

// ShortDateToTimestamp will return a timestamp from a time truncated time to the day
func ShortDateToTimestamp(tv time.Time) *timestamp.Timestamp {
	tv = tv.Truncate(time.Hour * 24)
	ts, err := ptypes.TimestampProto(tv)
	if err != nil {
		return nil
	}
	return ts
}

// DateFromEpoch returns a time.Time from an epoch value in milliseconds
func DateFromEpoch(t int64) time.Time {
	return time.Unix(0, t*1000000)
}

// TimestampFromEpoch will return a timestamp from an epoch value in milliseconds
func TimestampFromEpoch(t int64) *timestamp.Timestamp {
	return ToTimestamp(DateFromEpoch(t))
}

// ShortDateFromEpoch will return a short date from a epoch value in milliseconds
func ShortDateFromEpoch(t int64) string {
	tv := DateFromEpoch(t)
	return tv.UTC().Format("2006-01-02")
}

// ShortDateFromTime will return a short date from a time
func ShortDateFromTime(tv time.Time) string {
	return tv.UTC().Format("2006-01-02")
}

// ShortDate returns a DATE (no time) formatted string from RFC 3339 formatted string
func ShortDate(date string) string {
	if strings.Contains(date, "T") {
		t, err := time.Parse("2006-01-02T15:04:05Z", date)
		if err != nil {
			return fmt.Sprintf("<error parsing date: %s. %v>", date, err)
		}
		return t.UTC().Format("2006-01-02")
	}
	return date
}

// ToTimestamp converts a string to a protobuf Timestamp or nil if it can't be converted
func ToTimestamp(v interface{}) *timestamp.Timestamp {
	if v == nil {
		return nil
	}
	tv := orm.ToSQLDate(v)
	if tv.Valid {
		return orm.ToTimestamp(tv)
	}
	return nil
}

// ToTimeRange will return a start and end time range in epoch using tv as the reference day and adding days (use negative number to subtract)
func ToTimeRange(tv time.Time, days int) (int64, int64) {
	tv = tv.UTC()
	end := time.Date(tv.Year(), tv.Month(), tv.Day(), 23, 59, 59, 9999, time.UTC)
	startend := time.Date(tv.Year(), tv.Month(), tv.Day(), 0, 0, 0, 0, time.UTC)
	start := startend.AddDate(0, 0, days)
	return TimeToEpoch(start), TimeToEpoch(end)
}

// GetSignalDate returns a metric date in short form for a time unit from the ref date
func GetSignalDate(timeUnit int32, refDate time.Time) string {
	switch timeUnit {
	case SignalTimeUnit_NOW:
		{
			return ShortDateFromTime(refDate)
		}
	case SignalTimeUnit_MONTH:
		{
			return ShortDateFromTime(refDate.AddDate(0, 0, -30))
		}
	case SignalTimeUnit_BIMONTH:
		{
			return ShortDateFromTime(refDate.AddDate(0, 0, -60))
		}
	case SignalTimeUnit_QUARTER:
		{
			return ShortDateFromTime(refDate.AddDate(0, 0, -90))
		}
	case SignalTimeUnit_HALFYEAR:
		{
			return ShortDateFromTime(refDate.AddDate(0, 0, -180))
		}
	case SignalTimeUnit_THIRDQUARTER:
		{
			return ShortDateFromTime(refDate.AddDate(0, 0, -270))
		}
	case SignalTimeUnit_YEAR:
		{
			return ShortDateFromTime(refDate.AddDate(0, 0, -365))
		}
	}
	return ""
}

// GetSignalTime returns a metric date for a time unit from the ref date
// This will be changed from -30 to -29 because of the next example
// Lets say I want to take 1 day from "yesterday at the end" lets say "2017-02-26 23:59:59.9999"
// that will result in "2017-02-25 23:59:59.9999" and after truncate "2017-02-25 00:00:00" so it is actually taken 2 days
func GetSignalTime(timeUnit int32, refDate time.Time) time.Time {
	var t time.Time
	switch timeUnit {
	case SignalTimeUnit_NOW:
		{
			return refDate.UTC().Truncate(time.Hour * 24)
		}
	case SignalTimeUnit_MONTH:
		{
			t = refDate.UTC().AddDate(0, 0, -30)
		}
	case SignalTimeUnit_BIMONTH:
		{
			t = refDate.UTC().AddDate(0, 0, -60)
		}
	case SignalTimeUnit_QUARTER:
		{
			t = refDate.UTC().AddDate(0, 0, -90)
		}
	case SignalTimeUnit_HALFYEAR:
		{
			t = refDate.UTC().AddDate(0, 0, -180)
		}
	case SignalTimeUnit_THIRDQUARTER:
		{
			t = refDate.UTC().AddDate(0, 0, -270)
		}
	case SignalTimeUnit_YEAR:
		{
			t = refDate.UTC().AddDate(0, 0, -365)
		}
	}

	return t.Truncate(time.Hour * 24)
}

// ToMillSec Convert time to milliseconds int64
func ToMilliSec(date time.Time) int64 {
	return date.UnixNano() / 1000000
}

func AddDaysToStrDate(date string, days int) (string, error) {
	d, err := time.Parse("2006-01-02", date)
	if err != nil {
		return "", err
	}
	return ShortDateFromTime(d.AddDate(0, 0, days)), nil
}

// Date represents the object structure for date
type Date struct {
	// Epoch the date in epoch format
	Epoch int64 `json:"epoch" bson:"epoch" yaml:"epoch" faker:"-"`
	// Offset the timezone offset from GMT
	Offset int64 `json:"offset" bson:"offset" yaml:"offset" faker:"-"`
	// Rfc3339 the date in RFC3339 format
	Rfc3339 string `json:"rfc3339" bson:"rfc3339" yaml:"rfc3339" faker:"-"`
}

// NewDateNow returns a Date object as of now
func NewDateNow() Date {
	epoch := EpochNow()
	val := DateFromEpoch(epoch).Format(time.RFC3339Nano)
	tv, _ := ISODateToTime(val)
	_, timezone := tv.Zone()
	return Date{
		Epoch:   epoch,
		Rfc3339: val,
		Offset:  int64(timezone),
	}
}

// NewDate returns a new Date object from a string date value
func NewDate(val string) (*Date, error) {
	tv, err := ISODateToTime(val)
	if err != nil {
		return nil, err
	}
	_, timezone := tv.Zone()
	return &Date{
		Epoch:   TimeToEpoch(tv),
		Rfc3339: tv.Round(time.Millisecond).Format(time.RFC3339Nano),
		Offset:  int64(timezone),
	}, nil
}
