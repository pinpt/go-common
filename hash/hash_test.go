package hash

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Checks that NewStream with .gz does not leak open files. This returns too many open files error if NewStream is not using writerWrapper
func TestHashNil(t *testing.T) {
	assert := assert.New(t)
	hashFunc := func() {
		Values(nil)
	}
	assert.NotPanics(hashFunc)
	assert.Equal("ef46db3751d8e999", Values(nil, nil, nil))
}

func TestHashInts(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("b7b41276360564d4", Values(int(1)))
	assert.Equal("b7b41276360564d4", Values(int8(1)))
	assert.Equal("b7b41276360564d4", Values(int16(1)))
	assert.Equal("b7b41276360564d4", Values(int32(1)))
	assert.Equal("b7b41276360564d4", Values(int64(1)))
	p1 := int(1)
	p2 := int8(1)
	p3 := int16(1)
	p4 := int32(1)
	p5 := int64(1)
	assert.Equal("b7b41276360564d4", Values(&p1))
	assert.Equal("b7b41276360564d4", Values(&p2))
	assert.Equal("b7b41276360564d4", Values(&p3))
	assert.Equal("b7b41276360564d4", Values(&p4))
	assert.Equal("b7b41276360564d4", Values(&p5))
	var i1 *int
	var i2 *int8
	var i3 *int16
	var i4 *int32
	var i5 *int64
	assert.Equal("ef46db3751d8e999", Values(i1))
	assert.Equal("ef46db3751d8e999", Values(i2))
	assert.Equal("ef46db3751d8e999", Values(i3))
	assert.Equal("ef46db3751d8e999", Values(i4))
	assert.Equal("ef46db3751d8e999", Values(i5))
}

func TestHashFloats(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("b7b41276360564d4", Values(float32(1)))
	assert.Equal("b7b41276360564d4", Values(float64(1)))
	assert.Equal("4a1109734a1f6e14", Values(float32(1.02)))
	assert.Equal("4a1109734a1f6e14", Values(float64(1.02)))
	p1 := float32(1.02)
	p2 := float64(1.02)
	assert.Equal("4a1109734a1f6e14", Values(&p1))
	assert.Equal("4a1109734a1f6e14", Values(&p2))
	p1 = float32(1)
	p2 = float64(1)
	assert.Equal("b7b41276360564d4", Values(&p1))
	assert.Equal("b7b41276360564d4", Values(&p2))
	var f1 *float32
	var f2 *float64
	assert.Equal("ef46db3751d8e999", Values(f1))
	assert.Equal("ef46db3751d8e999", Values(f2))
}

func TestHashString(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("d24ec4f1a98c6e5b", Values("a"))
	p := "a"
	assert.Equal("d24ec4f1a98c6e5b", Values(&p))
	var s *string
	assert.Equal("ef46db3751d8e999", Values(s))
	assert.Equal("ef46db3751d8e999", Values(nil))
	assert.Equal("ef46db3751d8e999", Values(""))
	assert.Equal("ef46db3751d8e999", Values((*string)(nil)))
	assert.Equal("2c5e0532cab8422c", Values(strings.Repeat("*", 64)))
}

func TestHashBool(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("d7c9b97948142e4a", Values(true))
	assert.Equal("6d3f99ccc0c03a7a", Values(false))
	var b bool
	assert.Equal("6d3f99ccc0c03a7a", Values(&b))
	b = true
	assert.Equal("d7c9b97948142e4a", Values(&b))
	var b1 *bool
	assert.Equal("ef46db3751d8e999", Values(b1))
}

type testStruct struct {
	A string
	B string
}

func TestStruct(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("7478cecbac1f6839", Values(&testStruct{"a", "b"}))
	assert.Equal("7478cecbac1f6839", Values(testStruct{"a", "b"}))
}

func TestStringSlice(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("3c697d223fa7e885", Values([]string{"1", "2", "3"}))
}

func TestMap(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("16a930acf9cc7ae7", Values(map[string]interface{}{"a": 1, "b": "2"}))
	assert.Equal("16a930acf9cc7ae7", Values(map[string]string{"a": "1", "b": "2"}))
}

func TestByteBuffer(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("e66ae7354fcfee98", Values([]byte("ABC")))
	assert.Equal("e66ae7354fcfee98", Values("ABC"))
}

func TestInterface(t *testing.T) {
	assert := assert.New(t)
	var i interface{} = "ABC"
	assert.Equal("e66ae7354fcfee98", Values(i))
	assert.Equal("bca81cb4e2d6ad15", Values(&i)) // because it's converted to json
}

func TestSum64(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("ea8842e9ea2638fa", Values("hi"))
	assert.Equal(uint64(16899831174130972922), Sum64("ea8842e9ea2638fa"))
	assert.Equal(0, Modulo("ea8842e9ea2638fa", 1))
	assert.Equal(0, Modulo("ea8842e9ea2638fa", 2))
	assert.Equal(1, Modulo("ea8842e9ea2638fa", 3))
	assert.Equal(0, Modulo("ea8842e9ea2638fa", 4))
	assert.Equal(2, Modulo("ea8842e9ea2638fa", 5))
	assert.Equal(4, Modulo("ea8842e9ea2638fa", 6))
	assert.Equal(3, Modulo("ea8842e9ea2638fa", 7))
	assert.Equal(0, Modulo("ea8842e9ea2638fa", 8))
	assert.Equal(4, Modulo("ea8842e9ea2638fa", 9))
	assert.Equal(2, Modulo("ea8842e9ea2638fa", 10))
}
