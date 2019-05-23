package json

import (
	"testing"
	"time"

	"github.com/oliveagle/jsonpath"
	"github.com/pinpt/go-common/datetime"
	"github.com/stretchr/testify/assert"
)

func TestInvokeActionStatic(t *testing.T) {
	assert := assert.New(t)
	result, err := invokeAction("hi", map[string]interface{}{})
	assert.NoError(err)
	assert.Equal("hi", result)
}

func TestInvokeActionEpoch(t *testing.T) {
	assert := assert.New(t)
	result, err := invokeAction("epoch($.date)", map[string]interface{}{"date": "2019-01-23T22:09:58Z"})
	assert.NoError(err)
	assert.Equal(int64(1548281398000), result)
	result, err = invokeAction(`epoch($.date, "2006-01-02T15:04:05Z")`, map[string]interface{}{"date": "2019-01-23T22:09:58Z"})
	assert.NoError(err)
	assert.Equal(int64(1548281398000), result)
	result, err = invokeAction(`epoch()`, map[string]interface{}{})
	assert.NoError(err)
	assert.NotNil(result)
	assert.WithinDuration(time.Now(), datetime.DateFromEpoch(result.(int64)), time.Millisecond)
	result, err = invokeAction(`epoch($.date, "2006-01-02T15:04:05Z")`, map[string]interface{}{})
	assert.NoError(err)
	assert.Equal(int64(0), result)
	result, err = invokeAction(`epoch($.date, "2006-01-02T15:04:05Z")`, map[string]interface{}{"date": ""})
	assert.NoError(err)
	assert.Equal(int64(0), result)
	result, err = invokeAction(`epoch($.date, 2006-01-02 03:04:05.0)`, map[string]interface{}{"date": "2018-08-31 01:01:05.0"})
	assert.NoError(err)
	assert.Equal(int64(1535677265000), result)
	result, err = invokeAction(`epoch($.date, 2006-01-02 03:04:05.0, 2006-01-02)`, map[string]interface{}{"date": "2018-08-31"})
	assert.NoError(err)
	assert.Equal(int64(1535673600000), result)
}

func TestInvokeActionString(t *testing.T) {
	assert := assert.New(t)
	result, err := invokeAction("string($.date)", map[string]interface{}{"date": 1548281398000})
	assert.NoError(err)
	assert.Equal("1548281398000", result)
}

func TestInvokeActionHash(t *testing.T) {
	assert := assert.New(t)
	result, err := invokeAction("hash($.date, $.id)", map[string]interface{}{"date": 1548281398000, "id": "234"})
	assert.NoError(err)
	assert.Equal("18121570b4652185", result)
	result, err = invokeAction("hash($.date, $.id, 1)", map[string]interface{}{"date": 1548281398000, "id": "234"})
	assert.NoError(err)
	assert.Equal("22d9859cacf7a9c6", result)
}

func TestInvokeActionHashIf(t *testing.T) {
	assert := assert.New(t)
	result, err := invokeAction("hash_if($.foo, $.date, $.id)", map[string]interface{}{"date": 1548281398000, "id": "234"})
	assert.NoError(err)
	assert.Nil(result)
	result, err = invokeAction("hash_if($.date, $.date, $.id, 1)", map[string]interface{}{"date": 1548281398000, "id": "234"})
	assert.NoError(err)
	assert.Equal("22d9859cacf7a9c6", result)
}

func TestInvokeActionStringStaticArg(t *testing.T) {
	assert := assert.New(t)
	result, err := invokeAction("string(1548281398000)", map[string]interface{}{})
	assert.NoError(err)
	assert.Equal("1548281398000", result)
}

func TestInvokeActionNoAction(t *testing.T) {
	assert := assert.New(t)
	result, err := invokeAction("$.foo", map[string]interface{}{"foo": "bar"})
	assert.NoError(err)
	assert.Equal("bar", result)
}

func TestJSONPathWithNil(t *testing.T) {
	assert := assert.New(t)
	val, err := jsonpath.JsonPathLookup(map[string]interface{}{}, "$.foo")
	assert.True(isJSONPathNotFound(err))
	assert.True(isJSONPathNil(val))
	val, err = jsonpath.JsonPathLookup(map[string]interface{}{}, "$.foo.bar")
	assert.True(isJSONPathNotFound(err))
	assert.True(isJSONPathNil(val))
}

func TestCoalesce(t *testing.T) {
	assert := assert.New(t)
	result, err := invokeAction("coalesce($.a, $b)", map[string]interface{}{"a": "bar"})
	assert.NoError(err)
	assert.Equal("bar", result)
	result, err = invokeAction("coalesce($.a, $b)", map[string]interface{}{"b": "bar"})
	assert.NoError(err)
	assert.Equal("bar", result)
	result, err = invokeAction("coalesce($.a, $b, bar)", map[string]interface{}{"c": "bar"})
	assert.NoError(err)
	assert.Equal("bar", result)
	result, err = invokeAction("coalesce($.a, $b, foo)", map[string]interface{}{"a": 0, "b": "bar"})
	assert.NoError(err)
	assert.Equal("bar", result)
	result, err = invokeAction("coalesce($.a, $b)", map[string]interface{}{})
	assert.NoError(err)
	assert.Nil(result)
}
