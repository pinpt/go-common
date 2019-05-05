package number

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToBool(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal(true, ToBool("true"))
	assert.Equal(true, ToBool("1"))
	assert.Equal(false, ToBool("false"))
	assert.Equal(false, ToBool("0"))
	assert.Equal(false, ToBool("x"))
}

func TestToBoolAny(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal(true, ToBoolAny("true"))
	assert.Equal(true, ToBoolAny("1"))
	assert.Equal(false, ToBoolAny("false"))
	assert.Equal(false, ToBoolAny("0"))
	assert.Equal(false, ToBoolAny("x"))
	assert.Equal(false, ToBoolAny(0))
	assert.Equal(false, ToBoolAny(int32(0)))
	assert.Equal(false, ToBoolAny(int64(0)))
	assert.Equal(false, ToBoolAny(float32(0)))
	assert.Equal(false, ToBoolAny(float64(0)))
	assert.Equal(true, ToBoolAny(int32(1)))
	assert.Equal(true, ToBoolAny(int64(1)))
	assert.Equal(true, ToBoolAny(float32(1)))
	assert.Equal(true, ToBoolAny(float64(1)))
	assert.Equal(true, ToBoolAny(true))
	assert.Equal(false, ToBoolAny(false))
	assert.Equal(true, ToBoolAny(BoolPointer(true)))
	assert.Equal(false, ToBoolAny(BoolPointer(false)))
}

func TestToInt32(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal(int32(123), ToInt32("123"))
	assert.Equal(int32(0), ToInt32("123.0"))
	assert.Equal(int32(0), ToInt32(""))
	assert.Equal(int32(-123), ToInt32("-123"))
}
func TestToInt32Any(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal(int32(123), ToInt32Any("123"))
	assert.Equal(int32(123), ToInt32Any(123))
	assert.Equal(int32(123), ToInt32Any(int32(123)))
	assert.Equal(int32(123), ToInt32Any(int64(123)))
	assert.Equal(int32(123), ToInt32Any(float32(123)))
	assert.Equal(int32(123), ToInt32Any(float64(123)))
	assert.Equal(int32(0), ToInt32Any("123.0"))
	assert.Equal(int32(0), ToInt32Any(""))
	assert.Equal(int32(-123), ToInt32Any("-123"))
}

func TestToInt64(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal(int64(123), ToInt64("123"))
	assert.Equal(int64(0), ToInt64("123.0"))
	assert.Equal(int64(0), ToInt64(""))
	assert.Equal(int64(-123), ToInt64("-123"))
}

func TestToInt64Any(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal(int64(123), ToInt64Any("123"))
	assert.Equal(int64(123), ToInt64Any(123))
	assert.Equal(int64(123), ToInt64Any(int32(123)))
	assert.Equal(int64(123), ToInt64Any(int64(123)))
	assert.Equal(int64(123), ToInt64Any(float32(123)))
	assert.Equal(int64(123), ToInt64Any(float64(123)))
	assert.Equal(int64(0), ToInt64Any("123.0"))
	assert.Equal(int64(0), ToInt64Any(""))
	assert.Equal(int64(-123), ToInt64Any("-123"))
}

func TestNullString(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal("123", NullString("123"))
	assert.Equal("123", NullString(123))
	s := "123"
	assert.Equal("123", NullString(&s))
	var ss *string
	assert.Equal("NULL", NullString(ss))
	s = ""
	ss = &s
	assert.Equal("NULL", NullString(ss))
	assert.Equal("NULL", NullString(nil))
	assert.Equal("NULL", NullString(""))
	var int32Int int32
	assert.Equal("NULL", NullString(int32Int))
	var intInt int
	assert.Equal("NULL", NullString(intInt))
	var intInt64 int64
	assert.Equal("NULL", NullString(intInt64))
}

func TestBoolToStringAsInt(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal("1", BoolToStringAsInt(true))
	assert.Equal("0", BoolToStringAsInt(false))
}

func TestBoolPointer(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	in := true
	assert.Equal(&in, BoolPointer(in))
	in = false
	assert.Equal(&in, BoolPointer(in))
}

// I took the lower and the hights numbers for the type int64
func TestInt64Pointer(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	var lowLimit int64 = -9223372036854775808
	var highLimit int64 = 9223372036854775807
	var ceroInt64 int64

	assert.Equal((*int64)(nil), Int64Pointer(ceroInt64))
	assert.Equal(&lowLimit, Int64Pointer(lowLimit))
	assert.Equal(&highLimit, Int64Pointer(highLimit))
}

func TestInt32Pointer(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	var lowLimit int32 = -2147483648
	var highLimit int32 = 2147483647
	var ceroInt32 int32

	assert.Equal((*int32)(nil), Int32Pointer(ceroInt32))
	assert.Equal(&lowLimit, Int32Pointer(lowLimit))
	assert.Equal(&highLimit, Int32Pointer(highLimit))
}

func TestIntPointer(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	var lowLimit int = -2147483648
	var highLimit int = 2147483647
	var ceroInt int

	assert.Equal((*int)(nil), IntPointer(ceroInt))
	assert.Equal(&lowLimit, IntPointer(lowLimit))
	assert.Equal(&highLimit, IntPointer(highLimit))
}

func TestToBytesSize(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal("79 MB", ToBytesSize(82854982))
	assert.Equal("7.9 MB", ToBytesSize(8285982))
	assert.Equal("1 B", ToBytesSize(1))
	// This case will increase 1% of coverage if the sizes
	// array in ToBytesSizeBigInt function reduces two values
	// "ZB" and "YB"
	assert.Equal("8.0 EB", ToBytesSize(9223372036854775807))
}

func TestPercentage(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal(float64(0), Percent(0, 0))
	assert.Equal(float64(0), Percent(10, 0))
	assert.Equal(float64(1), Percent(10, 10))
	assert.Equal(float64(2), Percent(20, 10))
	assert.Equal(float64(0), Percent(0, 10))
}

func TestPercentage32(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal(float64(0), Percent32(0, 0))
	assert.Equal(float64(0), Percent32(10, 0))
	assert.Equal(float64(1), Percent32(10, 10))
	assert.Equal(float64(2), Percent32(20, 10))
	assert.Equal(float64(0), Percent32(0, 10))
}

func TestPercentageFloat32(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal(float64(0), PercentFloat32(0, 0))
	assert.Equal(float64(0), PercentFloat32(10, 0))
	assert.Equal(float64(1), PercentFloat32(10, 10))
	assert.Equal(float64(2), PercentFloat32(20, 10))
	assert.Equal(float64(0), PercentFloat32(0, 10))
}

func TestPercentageFloat64(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal(float64(0), PercentFloat64(0, 0))
	assert.Equal(float64(0), PercentFloat64(10, 0))
	assert.Equal(float64(1), PercentFloat64(10, 10))
	assert.Equal(float64(2), PercentFloat64(20, 10))
	assert.Equal(float64(0), PercentFloat64(0, 10))
}
