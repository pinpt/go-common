package datetime

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestISODate(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	tv := time.Now()
	td := ISODate()
	ts, err := ISODateToTime(td)
	assert.Nil(err)
	assert.Equal(ISODateFromTime(tv), td)
	assert.Equal(ISODateFromTime(ts), ISODateFromTime(tv))
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

	var dates = []struct {
		date string
	}{
		{"2006-02-02T15:04:05-0600"},
		{"2006-02-02T15:04:05.9999-0600"},
		{"2006-02-02T15:04:05.99999-06:00"},
		{"2006-02-02T15:04:05-06:00"},
		{"2006-02-02T15:04:05.999999Z-06:00"},
		{"2006-02-02T15:04:05.9Z-06:00"},
	}

	assert := assert.New(t)

	for _, tt := range dates {
		ok, err := ISODateOffsetToTime(tt.date)

		date := time.Date(int(2006), time.February, int(2), int(15), int(4), int(5), int(0), time.Local)

		assert.Nil(err)
		year1, month1, day1 := ok.Date()
		year2, month2, day2 := date.Date()
		assert.Equal(year1, year2)
		assert.Equal(month1, month2)
		assert.Equal(day1, day2)
	}

}

func TestISODateOffsetToTimeWithMillis(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	ok, err := ISODateOffsetToTime("2012-06-04T10:05:49.000-04:00")
	assert.NoError(err)
	assert.Equal("2012-06-04T14:05:49Z", ISODateFromTime(ok))
}

func TestISODateOffsetToTimeWithEmptyString(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	timeVal, err := ISODateOffsetToTime("")
	assert.NoError(err)
	assert.True(timeVal.IsZero())
}

func TestTimeToEpochWithEmptyTime(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	assert.EqualValues(0, TimeToEpoch(time.Time{}))
}

func TestShortDate(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	assert.Contains(ShortDate("2006-01-02415:04:035T"), "<error parsing date: ")
	assert.Contains(ShortDate("2006-01-02415:04:035"), "2006-01-02415:04:035")
}

func TestGetDateEmpty(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal("", GetSignalDate(int32(1000), time.Now()))
	assert.Equal(ShortDateFromTime(time.Now().AddDate(0, 0, -30)), GetSignalDate(SignalTimeUnitMONTH, time.Now()))
	assert.Equal(ShortDateFromTime(time.Now().AddDate(0, 0, -60)), GetSignalDate(SignalTimeUnitBIMONTH, time.Now()))
	assert.Equal(ShortDateFromTime(time.Now().AddDate(0, 0, -90)), GetSignalDate(SignalTimeUnitQUARTER, time.Now()))
	assert.Equal(ShortDateFromTime(time.Now().AddDate(0, 0, -180)), GetSignalDate(SignalTimeUnitHALFYEAR, time.Now()))
	assert.Equal(ShortDateFromTime(time.Now().AddDate(0, 0, -365)), GetSignalDate(SignalTimeUnitYEAR, time.Now()))
	assert.Equal(ShortDateFromTime(time.Now()), GetSignalDate(SignalTimeUnitNOW, time.Now()))
	assert.Equal(ShortDateFromTime(time.Now().AddDate(0, 0, -270)), GetSignalDate(SignalTimeUnitTHIRDQUARTER, time.Now()))
}

func TestGetSignalTime(t *testing.T) {
	t.Parallel()
	refTime, _ := time.Parse("2006-01-02T15:04:05Z", "2017-01-31T10:22:00Z")
	realRefDate := GetSignalTime(SignalTimeUnitNOW, refTime)
	assert := assert.New(t)
	assert.Equal("2017-01-31 00:00:00 +0000 UTC", realRefDate.String())
	assert.Equal("2017-01-01 00:00:00 +0000 UTC", GetSignalTime(SignalTimeUnitMONTH, realRefDate).String())
	assert.Equal("2016-12-02 00:00:00 +0000 UTC", GetSignalTime(SignalTimeUnitBIMONTH, realRefDate).String())
	assert.Equal("2016-11-02 00:00:00 +0000 UTC", GetSignalTime(SignalTimeUnitQUARTER, realRefDate).String())
	assert.Equal("2016-08-04 00:00:00 +0000 UTC", GetSignalTime(SignalTimeUnitHALFYEAR, realRefDate).String())
	assert.Equal("2016-02-01 00:00:00 +0000 UTC", GetSignalTime(SignalTimeUnitYEAR, realRefDate).String())
	assert.Equal("2016-05-06 00:00:00 +0000 UTC", GetSignalTime(SignalTimeUnitTHIRDQUARTER, realRefDate).String())
}

func TestDateObject(t *testing.T) {
	assert := assert.New(t)
	date1 := NewDateNow()
	dt := DateFromEpoch(date1.Epoch)
	_, tz := dt.Zone()
	assert.WithinDuration(dt, time.Now(), time.Second)
	assert.Equal(int64(tz)/60, date1.Offset)
	assert.Equal(dt.Format(RFC3339), date1.Rfc3339)
	date2, err := NewDate(date1.Rfc3339)
	assert.NoError(err)
	dt2 := DateFromEpoch(date2.Epoch)
	_, tz2 := dt2.Zone()
	assert.WithinDuration(dt2, time.Now(), time.Second)
	assert.Equal(int64(tz2)/60, date2.Offset)
	assert.Equal(dt2.Format(RFC3339), date2.Rfc3339) // this is failing
	_, err = NewDate("x")
	assert.True(strings.Contains(err.Error(), `parsing time "x"`))
	date3, err := NewDate(ISODate())
	assert.NoError(err)
	dt3 := DateFromEpoch(date3.Epoch)
	assert.WithinDuration(dt3, time.Now(), time.Second)
	assert.Equal(int64(0), date3.Offset) // make sure ISO returns GMT timezone
	dt4 := NewDateWithTime(dt)
	assert.Equal(date1.Epoch, dt4.Epoch)
}

func TestEndOfDay(t *testing.T) {
	assert := assert.New(t)
	date1 := NewDateNow()
	val := EndofDay(date1.Epoch)
	assert.NotEqual(date1.Epoch, val)
	t1, _ := ISODateToEpoch("2019-08-08T23:59:59.000Z")
	assert.Equal(int64(1565308799000), t1)
	t2, _ := ISODateToEpoch("2019-08-08T23:59:59.999Z")
	assert.Equal(int64(1565308799999), t2)
	t3, _ := ISODateToEpoch("2019-08-08T23:59:59.998Z")
	assert.Equal(int64(1565308799998), t3)
	t4, _ := ISODateToEpoch("2019-08-08T23:59:59.898Z")
	assert.Equal(int64(1565308799898), t4)
}

var githubRawDate = "2018-05-29T14:41:28Z"
var gihtubDatePreFormated = "2018-05-29T14:41:28.000000+00:00"
var dateNotExpected = "2018-05-29T14:41:28Z"
var dateExpected = "2018-05-29T14:41:28+00:00"

func TestError1(t *testing.T) {
	assert := assert.New(t)
	dt, err := NewDate(githubRawDate) // This is how we receive the raw date from github
	assert.NoError(err)
	assert.NotEqual(dateNotExpected, dt.Rfc3339)
}

func TestError2(t *testing.T) {
	assert := assert.New(t)
	dt, err := NewDate(gihtubDatePreFormated) // if I convert the date to different format
	assert.NoError(err)
	assert.NotEqual(dateNotExpected, dt.Rfc3339)
}

func solutionDate(val string) (*Date, error) {
	tv, err := ISODateToTime(val)
	if err != nil {
		return nil, err
	}
	_, timezone := tv.Zone()
	return &Date{
		Epoch:   TimeToEpoch(tv),
		Rfc3339: tv.Round(time.Millisecond).Format("2006-01-02T15:04:05.999999999-07:00"), // Posible format solution
		Offset:  int64(timezone) / 60,
	}, nil
}

func TestError1WithSolutionDate(t *testing.T) {
	assert := assert.New(t)
	dt, err := solutionDate(githubRawDate) // Using possible solution
	assert.NoError(err)
	assert.NotEqual(dateNotExpected, dt.Rfc3339)
	assert.Equal(dateExpected, dt.Rfc3339)
}

func TestErrorWithSolutionDate(t *testing.T) {
	assert := assert.New(t)
	dt, err := solutionDate(gihtubDatePreFormated) // Using possible solution
	assert.NoError(err)
	assert.NotEqual(dateNotExpected, dt.Rfc3339)
	assert.Equal(dateExpected, dt.Rfc3339)
}

func TestNewDateFromEpoch(t *testing.T) {
	assert := assert.New(t)
	expDate := NewDateNow()
	tstDate := NewDateFromEpoch(expDate.Epoch)
	assert.Equal(expDate.Epoch, tstDate.Epoch, "Epoch not expected")
	assert.Equal(expDate.Offset, tstDate.Offset, "Offset not expected")
	assert.Equal(expDate.Rfc3339, tstDate.Rfc3339, "Rfc3339 not expected")

}

func TestDateRange(t *testing.T) {
	assert := assert.New(t)
	day, err := time.Parse(time.RFC3339Nano, "2018-06-04T10:05:49.000-04:00")
	assert.NoError(err)
	start, end := DateRange(day, 5)
	assert.EqualValues(StartofDay(day.AddDate(0, 0, -4).Unix()*1000), start)
	assert.EqualValues(EndofDay(day.Unix()*1000), end)
}

func TestDateRangeAlltime(t *testing.T) {
	assert := assert.New(t)
	day, err := time.Parse(time.RFC3339Nano, "2018-06-04T10:05:49.000-04:00")
	assert.NoError(err)
	start, end := DateRange(day, -1)
	assert.EqualValues(0, start)
	assert.EqualValues(EndofDay(day.Unix()*1000), end)
}

func TestEpochSameMinute(t *testing.T) {
	assert := assert.New(t)
	// same time
	now := EpochNow()
	assert.True(EpochMinuteApart(now, now))
	// one second apart
	time1, err := time.Parse(time.RFC3339Nano, "2018-06-04T10:05:49.000-04:00")
	assert.NoError(err)
	time2, err := time.Parse(time.RFC3339Nano, "2018-06-04T10:05:50.000-04:00")
	assert.NoError(err)
	// different minutes
	assert.True(EpochMinuteApart(TimeToEpoch(time1), TimeToEpoch(time2)))
	time1, err = time.Parse(time.RFC3339Nano, "2018-06-04T10:05:49.000-04:00")
	assert.NoError(err)
	time2, err = time.Parse(time.RFC3339Nano, "2018-06-04T10:06:00.000-04:00")
	assert.NoError(err)
	// one exact minute apart
	assert.True(EpochMinuteApart(TimeToEpoch(time1), TimeToEpoch(time2)))
	time1, err = time.Parse(time.RFC3339Nano, "2018-06-04T10:05:49.000-04:00")
	assert.NoError(err)
	time2, err = time.Parse(time.RFC3339Nano, "2018-06-04T10:06:49.000-04:00")
	assert.NoError(err)
	// over one minute apart
	assert.True(EpochMinuteApart(TimeToEpoch(time1), TimeToEpoch(time2)))
	time1, err = time.Parse(time.RFC3339Nano, "2018-06-04T10:05:49.000-04:00")
	assert.NoError(err)
	time2, err = time.Parse(time.RFC3339Nano, "2018-06-04T10:06:49.100-04:00")
	assert.NoError(err)
	assert.False(EpochMinuteApart(TimeToEpoch(time1), TimeToEpoch(time2)))
}

func TestConvertToModel(t *testing.T) {
	assert := assert.New(t)

	refTime, _ := time.Parse("2006-01-02T15:04:05Z", "2017-01-31T10:22:00Z")
	var date Date

	ConvertToModel(refTime, &date)

	assert.Equal(int64(1485858120000), date.Epoch)
	assert.Equal(int64(0), date.Offset)
	assert.Equal("2017-01-31T10:22:00+00:00", date.Rfc3339)

	var timeZero time.Time
	var dateZero Date

	ConvertToModel(timeZero, &dateZero)

	assert.Equal(int64(0), dateZero.Epoch)
	assert.Equal(int64(0), dateZero.Offset)
	assert.Equal("", dateZero.Rfc3339)

}

func TestTimeFromDate(t *testing.T) {
	assert := assert.New(t)
	date := Date{
		Offset:  -240,
		Epoch:   1528121149000,
		Rfc3339: "2018-06-04T10:05:49-04:00",
	}
	tv := TimeFromDate(date)
	_, offset := tv.Zone()
	assert.Equal(-14400, offset)
	assert.EqualValues(date.Offset, offset/60)
	assert.EqualValues(date.Epoch, TimeToEpoch(tv))
	assert.EqualValues(date.Rfc3339, tv.Format(RFC3339))
	rf, _ := time.Parse(time.RFC3339, "2018-06-04T10:05:49-04:00")
	assert.EqualValues(rf.String(), tv.String())
}
