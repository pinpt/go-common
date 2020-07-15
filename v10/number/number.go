package number

import (
	"fmt"
	"math/big"
	"strconv"
)

// ToBool returns bool for string and if parse error, return false
func ToBool(v string) bool {
	if ok, err := strconv.ParseBool(v); err == nil {
		return ok
	}
	return false
}

// ToBoolAny returns bool for interface{} and if parse error, return false
func ToBoolAny(v interface{}) bool {
	if v, ok := v.(bool); ok {
		return v
	}
	if v, ok := v.(*bool); ok {
		if v != nil {
			return *v
		}
		return false
	}
	if v, ok := v.(int); ok {
		return v > 0
	}
	if v, ok := v.(int32); ok {
		return v > 0
	}
	if v, ok := v.(int64); ok {
		return v > 0
	}
	if v, ok := v.(float32); ok {
		return v > 0
	}
	if v, ok := v.(float64); ok {
		return v > 0
	}
	if v, ok := v.(*int); ok {
		if v != nil {
			return *v > 0
		}
	}
	if v, ok := v.(*int32); ok {
		if v != nil {
			return *v > 0
		}
	}
	if v, ok := v.(*int64); ok {
		if v != nil {
			return *v > 0
		}
	}
	if v, ok := v.(string); ok {
		return ToBool(v)
	}
	if v, ok := v.(*string); ok {
		if v != nil {
			return ToBool(*v)
		}
	}
	return false
}

// ToInt32 converts a string to a base 10 32-bit integer or return 0
func ToInt32(v string) int32 {
	if v, err := strconv.ParseInt(v, 10, 32); err == nil {
		return int32(v)
	}
	return 0
}

// ToInt32Any converts an interface{} to a base 10 32-bit integer or return 0
func ToInt32Any(v interface{}) int32 {
	if v, ok := v.(int); ok {
		return int32(v)
	}
	if v, ok := v.(int32); ok {
		return v
	}
	if v, ok := v.(int64); ok {
		return int32(v)
	}
	if v, ok := v.(float32); ok {
		return int32(v)
	}
	if v, ok := v.(float64); ok {
		return int32(v)
	}
	if v, ok := v.(*int); ok {
		if v != nil {
			return int32(*v)
		}
	}
	if v, ok := v.(*int32); ok {
		if v != nil {
			return *v
		}
	}
	if v, ok := v.(*int64); ok {
		if v != nil {
			return int32(*v)
		}
	}
	if v, ok := v.(string); ok {
		return ToInt32(v)
	}
	if v, ok := v.(*string); ok {
		if v != nil {
			return ToInt32(*v)
		}
	}
	return 0
}

// ToInt64 converts a string to a base 10 64-bit integer or return 0
func ToInt64(v string) int64 {
	if v, err := strconv.ParseInt(v, 10, 64); err == nil {
		return v
	}
	return 0
}

// ToInt64Any converts an interface{} to a base 10 64-bit integer or return 0
func ToInt64Any(v interface{}) int64 {
	if v, ok := v.(int); ok {
		return int64(v)
	}
	if v, ok := v.(int32); ok {
		return int64(v)
	}
	if v, ok := v.(int64); ok {
		return v
	}
	if v, ok := v.(float32); ok {
		return int64(v)
	}
	if v, ok := v.(float64); ok {
		return int64(v)
	}
	if v, ok := v.(*int); ok {
		if v != nil {
			return int64(*v)
		}
	}
	if v, ok := v.(*int32); ok {
		if v != nil {
			return int64(*v)
		}
	}
	if v, ok := v.(*int64); ok {
		if v != nil {
			return int64(*v)
		}
	}
	if v, ok := v.(string); ok {
		return ToInt64(v)
	}
	if v, ok := v.(*string); ok {
		if v != nil {
			return ToInt64(*v)
		}
	}
	return 0
}

// ToFloat64 converts a string to a 64-bit float or return 0
func ToFloat64(v string) float64 {
	if v, err := strconv.ParseFloat(v, 64); err == nil {
		return v
	}
	return 0
}

// ToFloat64Any converts an interface{} to a 64-bit float or return 0
func ToFloat64Any(v interface{}) float64 {
	if v, ok := v.(int); ok {
		return float64(v)
	}
	if v, ok := v.(int32); ok {
		return float64(v)
	}
	if v, ok := v.(int64); ok {
		return float64(v)
	}
	if v, ok := v.(float32); ok {
		return float64(v)
	}
	if v, ok := v.(float64); ok {
		return v
	}
	if v, ok := v.(*int); ok {
		if v != nil {
			return float64(*v)
		}
	}
	if v, ok := v.(*int32); ok {
		if v != nil {
			return float64(*v)
		}
	}
	if v, ok := v.(*int64); ok {
		if v != nil {
			return float64(*v)
		}
	}
	if v, ok := v.(string); ok {
		return ToFloat64(v)
	}
	if v, ok := v.(*string); ok {
		if v != nil {
			return ToFloat64(*v)
		}
	}
	return 0.0
}

// BoolToStringAsInt returns a 1 or 0 string for a bool
func BoolToStringAsInt(bo bool) string {
	if bo {
		return "1"
	}
	return "0"
}

// BoolPointer will return a pointer to the bool value
func BoolPointer(v bool) *bool {
	return &v
}

// Int64Pointer will return the address of the value as a pointer or nil if 0
func Int64Pointer(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}

// Int32Pointer will return the address of the value as a pointer or nil if 0
func Int32Pointer(v int32) *int32 {
	if v == 0 {
		return nil
	}
	return &v
}

// IntPointer will return the address of the value as a pointer or nil if 0
func IntPointer(v int) *int {
	if v == 0 {
		return nil
	}
	return &v
}

// Float32Pointer returns a float64 as pointer
func Float32Pointer(v float32) *float32 {
	return &v
}

// Float64Pointer returns a float64 as pointer
func Float64Pointer(v float64) *float64 {
	return &v
}

// NullString will return NULL if the value is empty or the value if not
func NullString(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	if s, ok := value.(string); ok {
		if s == "" {
			return "NULL"
		}
		return s
	}
	if s, ok := value.(*string); ok {
		if s == nil || *s == "" {
			return "NULL"
		}
		return *s
	}
	if s, ok := value.(int); ok {
		if s == 0 {
			return "NULL"
		}
	}
	if s, ok := value.(int32); ok {
		if s == 0 {
			return "NULL"
		}
	}
	if s, ok := value.(int64); ok {
		if s == 0 {
			return "NULL"
		}
	}
	return fmt.Sprintf("%v", value)
}

//
// some code below is borrowed from https://github.com/dustin/go-humanize/blob/master/bigbytes.go
//
/*
Copyright (c) 2005-2008  Dustin Sallings <dustin@spy.net>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

<http://www.opensource.org/licenses/mit-license.php>
*/

var (
	bigIECExp = big.NewInt(1024)

	// BigByte is one byte in bit.Ints
	BigByte = big.NewInt(1)
	// BigKByte is 1,024 bytes in bit.Ints
	BigKByte = (&big.Int{}).Mul(BigByte, bigIECExp)
	// BigMByte is 1,024 k bytes in bit.Ints
	BigMByte = (&big.Int{}).Mul(BigKByte, bigIECExp)
	// BigGByte is 1,024 m bytes in bit.Ints
	BigGByte = (&big.Int{}).Mul(BigMByte, bigIECExp)
	// BigTByte is 1,024 g bytes in bit.Ints
	BigTByte = (&big.Int{}).Mul(BigGByte, bigIECExp)
	// BigPByte is 1,024 t bytes in bit.Ints
	BigPByte = (&big.Int{}).Mul(BigTByte, bigIECExp)
	// BigEByte is 1,024 p bytes in bit.Ints
	BigEByte = (&big.Int{}).Mul(BigPByte, bigIECExp)
	// BigZByte is 1,024 e bytes in bit.Ints
	BigZByte = (&big.Int{}).Mul(BigEByte, bigIECExp)
	// BigYByte is 1,024 z bytes in bit.Ints
	BigYByte = (&big.Int{}).Mul(BigZByte, bigIECExp)
)

var ten = big.NewInt(10)

// order of magnitude (to a max order)
func oomm(n, b *big.Int, maxmag int) (float64, int) {
	mag := 0
	m := &big.Int{}
	for n.Cmp(b) >= 0 {
		n.DivMod(n, b, m)
		mag++
		if mag == maxmag && maxmag >= 0 {
			break
		}
	}
	return float64(n.Int64()) + (float64(m.Int64()) / float64(b.Int64())), mag
}

func humanateBigBytes(s, base *big.Int, sizes []string) string {
	if s.Cmp(ten) < 0 {
		return fmt.Sprintf("%d B", s)
	}
	c := (&big.Int{}).Set(s)
	val, mag := oomm(c, base, len(sizes)-1)
	suffix := sizes[mag]
	f := "%.0f %s"
	if val < 10 {
		f = "%.1f %s"
	}

	return fmt.Sprintf(f, val, suffix)
}

// ToBytesSizeBigInt produces a human readable representation of an bytes size
//
//
// ToBytesSizeBigInt(big.NewInt(82854982)) -> 83 MB
func ToBytesSizeBigInt(s *big.Int) string {
	sizes := []string{"B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"}
	return humanateBigBytes(s, bigIECExp, sizes)
}

// ToBytesSize produces a human readable representation of an bytes size
//
//
// ToBytesSize(82854982) -> 83 MB
func ToBytesSize(v int64) string {
	return ToBytesSizeBigInt(big.NewInt(v))
}

// Percent will safely return a percentage for two numbers diving a / b
func Percent(a int, b int) float64 {
	if b > 0 {
		return float64(a) / float64(b)
	}
	return 0
}

// Percent32 will safely return a percentage for two numbers diving a / b
func Percent32(a int32, b int32) float64 {
	if b > 0 {
		return float64(a) / float64(b)
	}
	return 0
}

// Percent64 will safely return a percentage for two numbers diving a / b
func Percent64(a int64, b int64) float64 {
	if b > 0 {
		return float64(a) / float64(b)
	}
	return 0
}

// PercentFloat32 will safely return a percentage for two numbers diving a / b
func PercentFloat32(a float32, b float32) float64 {
	if b > 0 {
		return float64(a) / float64(b)
	}
	return 0
}

// PercentFloat64 will safely return a percentage for two numbers diving a / b
func PercentFloat64(a float64, b float64) float64 {
	if b > 0 {
		return a / b
	}
	return 0
}
