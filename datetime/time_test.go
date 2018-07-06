package datetime

import (
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/assert"
)

func TestISODate(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	tv := time.Now()
	td := ISODate()
	ts, err := ISODateToTime(td)
	now := ToTimestamp("now")
	st := mysql.NullTime{Time: tv, Valid: true}
	assert.Nil(err)
	assert.Equal(ISODateFromTime(tv), td)
	assert.Equal(ISODateFromTime(ts), ISODateFromTime(tv))
	assert.Equal(ISODateFromTime(ts), ISODateFromTimestamp(now))
	assert.Equal(ISODateFromSQLNullTime(st), td)
	assert.Equal(ShortDate(td), ShortDateFromTimestamp(now))
}

func TestTimestampToEpoch(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	tv := time.Now()
	now := ToTimestamp(tv)
	t.Log(TimestampToEpoch(now))
	assert.Equal(tv.UnixNano()/1000000, TimestampToEpoch(now))
}

func TestShortDateFromEpoch(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	tv := time.Now()
	now := ToTimestamp(tv)
	assert.Equal(ShortDateFromTimestamp(ToTimestamp(tv)), ShortDateFromEpoch(TimestampToEpoch(now)))
}

func TestToTimeRange(t *testing.T) {
	t.Parallel()
	tv := time.Now().UTC()
	start, end := ToTimeRange(tv, 0)
	shortdate := ShortDateFromTime(tv)
	assert := assert.New(t)
	assert.Equal(shortdate+"T00:00:00Z", DateFromEpoch(start).UTC().Format("2006-01-02T15:04:05Z"))
	assert.Equal(shortdate+"T23:59:59Z", DateFromEpoch(end).UTC().Format("2006-01-02T15:04:05Z"))
}

func TestISODateToEpoch(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	ok, err := ISODateToEpoch("2017-11-21T18:40:21.091Z")

	assert.Nil(err)
	assert.Equal(int64(1511289621091), ok)

	ok, err = ISODateToEpoch("")
	assert.Nil(err)
	assert.Equal(int64(0), ok)

	ok, err = ISODateToEpoch("asdf")
	assert.Error(err)
	assert.Equal(int64(0), ok)
}

func TestISODateOffsetToTime(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	ok, err := ISODateOffsetToTime("2006-02-02T15:04:05-0600")

	date := time.Date(int(2006), time.February, int(2), int(15), int(4), int(5), int(0), time.Local)

	assert.Nil(err)
	year1, month1, day1 := ok.Date()
	year2, month2, day2 := date.Date()
	assert.Equal(year1, year2)
	assert.Equal(month1, month2)
	assert.Equal(day1, day2)
}

func TestISODateOffsetToTimeWithMillis(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	ok, err := ISODateOffsetToTime("2012-06-04T10:05:49.000-04:00")
	assert.NoError(err)
	assert.Equal("2012-06-04T14:05:49Z", ISODateFromTime(ok))
}

func TestShortDateToTimestamp(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	date := time.Date(int(2006), time.February, int(2), int(15), int(4), int(5), int(0), time.Local)

	ok := ShortDateToTimestamp(date)

	assert.Equal(&timestamp.Timestamp{Seconds: 1138838400, Nanos: 0}, ok)

	date = time.Date(int(0), time.February, int(2), int(15), int(4), int(5), int(0), time.Local)

	ok = ShortDateToTimestamp(date)

	assert.Nil(ok)
}

func TestTimestampFromEpoch(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	assert.Equal(&timestamp.Timestamp{Seconds: 1511296725, Nanos: 531000000}, TimestampFromEpoch(1511296725531))
}

func TestShortDateFromTimestamp(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	assert.Equal("", ShortDateFromTimestamp(nil))
}

func TestShortDate(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	assert.Contains(ShortDate("2006-01-02415:04:035T"), "<error parsing date: ")
	assert.Contains(ShortDate("2006-01-02415:04:035"), "2006-01-02415:04:035")
}

func TestToTimestamp(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	assert.Nil(ToTimestamp(nil))
}

func TestGetDateEmpty(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal("", GetSignalDate(int32(1000), time.Now()))
	assert.Equal(ShortDateFromTime(time.Now().AddDate(0, 0, -30)), GetSignalDate(SignalTimeUnit_MONTH, time.Now()))
	assert.Equal(ShortDateFromTime(time.Now().AddDate(0, 0, -60)), GetSignalDate(SignalTimeUnit_BIMONTH, time.Now()))
	assert.Equal(ShortDateFromTime(time.Now().AddDate(0, 0, -90)), GetSignalDate(SignalTimeUnit_QUARTER, time.Now()))
	assert.Equal(ShortDateFromTime(time.Now().AddDate(0, 0, -180)), GetSignalDate(SignalTimeUnit_HALFYEAR, time.Now()))
	assert.Equal(ShortDateFromTime(time.Now().AddDate(0, 0, -365)), GetSignalDate(SignalTimeUnit_YEAR, time.Now()))
	assert.Equal(ShortDateFromTime(time.Now()), GetSignalDate(SignalTimeUnit_NOW, time.Now()))
	assert.Equal(ShortDateFromTime(time.Now().AddDate(0, 0, -270)), GetSignalDate(SignalTimeUnit_THIRDQUARTER, time.Now()))
}

func TestGetSignalTime(t *testing.T) {
	t.Parallel()
	refTime, _ := time.Parse("2006-01-02T15:04:05Z", "2017-01-31T10:22:00Z")
	realRefDate := GetSignalTime(SignalTimeUnit_NOW, refTime)
	assert := assert.New(t)
	assert.Equal("2017-01-31 00:00:00 +0000 UTC", realRefDate.String())
	assert.Equal("2017-01-01 00:00:00 +0000 UTC", GetSignalTime(SignalTimeUnit_MONTH, realRefDate).String())
	assert.Equal("2016-12-02 00:00:00 +0000 UTC", GetSignalTime(SignalTimeUnit_BIMONTH, realRefDate).String())
	assert.Equal("2016-11-02 00:00:00 +0000 UTC", GetSignalTime(SignalTimeUnit_QUARTER, realRefDate).String())
	assert.Equal("2016-08-04 00:00:00 +0000 UTC", GetSignalTime(SignalTimeUnit_HALFYEAR, realRefDate).String())
	assert.Equal("2016-02-01 00:00:00 +0000 UTC", GetSignalTime(SignalTimeUnit_YEAR, realRefDate).String())
	assert.Equal("2016-05-06 00:00:00 +0000 UTC", GetSignalTime(SignalTimeUnit_THIRDQUARTER, realRefDate).String())
}
