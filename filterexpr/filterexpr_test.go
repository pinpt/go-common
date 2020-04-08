package filterexpr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInvalid(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile("a:")
	assert.EqualError(err, `1:3 (2): no match found, expected: "-", "/", "0", "\"", "false", "true", [ \t\r\n] or [1-9]`)
	assert.Nil(filter)
}

func TestSimpleKeyVal(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a:"b"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "b"}))
	assert.False(filter.Test(map[string]interface{}{"a": "a"}))
}

func TestSimpleKeyValWithSpaces(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a: "b"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "b"}))
	assert.False(filter.Test(map[string]interface{}{"a": "a"}))
	filter, err = Compile(`a : "b"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "b"}))
	assert.False(filter.Test(map[string]interface{}{"a": "a"}))
}

func TestSimpleKeyValEscaped(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a:"\"hello\""`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": `"hello"`}))
}

func TestSimpleKeyValNum(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a:123`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "123"}))
	assert.True(filter.Test(map[string]interface{}{"a": 123}))
	assert.False(filter.Test(map[string]interface{}{"a": "456"}))
}

func TestSimpleKeyValBool(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a:true`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": true}))
	assert.False(filter.Test(map[string]interface{}{"a": false}))
	filter, err = Compile(`a:false`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": false}))
	assert.False(filter.Test(map[string]interface{}{"a": true}))
}

func TestSimpleKeyValWithDots(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a.b:"true"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{
		"a": map[string]interface{}{
			"b": true,
		},
	}))
	assert.True(filter.Test(map[string]interface{}{
		"a": map[string]interface{}{
			"b": "true",
		},
	}))
	filter, err = Compile(`a.b:true`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{
		"a": map[string]interface{}{
			"b": true,
		},
	}))
	assert.True(filter.Test(map[string]interface{}{
		"a": map[string]interface{}{
			"b": "true",
		},
	}))
}

func TestWithOR(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a:"b" OR b:"a"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "b"}))
	assert.True(filter.Test(map[string]interface{}{"a": "a", "b": "a"}))
}

func TestWithAND(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a:"b" AND b:"a"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "b", "b": "a"}))
	assert.False(filter.Test(map[string]interface{}{"a": "b", "b": "c"}))
}

func TestWithANDGroup(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`(a:"a" OR b:"b") AND c:"c"`)
	assert.NoError(err)
	assert.False(filter.Test(map[string]interface{}{"a": "a", "b": "b"}))
	assert.True(filter.Test(map[string]interface{}{"a": "a", "b": "a", "c": "c"}))
	assert.True(filter.Test(map[string]interface{}{"a": "b", "b": "b", "c": "c"}))
}

func TestWithORGroup(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`(a:"a" OR b:"b") OR (c:"c" d:true)`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "a", "b": "b", "c": "c", "d": true}))
	assert.True(filter.Test(map[string]interface{}{"a": "b", "b": "b", "c": "d", "d": false}))
}

func TestRegexp(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a:/a/`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "a"}))
	assert.False(filter.Test(map[string]interface{}{"a": "ABC"}))
	filter, err = Compile(`a:/(?i)a/`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "ABC"}))
	filter, err = Compile(`a:/^a$/`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "a"}))
	assert.False(filter.Test(map[string]interface{}{"a": "b"}))
	filter, err = Compile(`a:/\d+/`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": 123}))
	filter, err = Compile(`a:/\d{1,3}/`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": 123}))
	filter, err = Compile(`a:/^admin\./`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "admin.Agent"}))
	assert.True(filter.Test(map[string]interface{}{"a": "admin.AgentLastUpdate"}))
	assert.False(filter.Test(map[string]interface{}{"a": "adminAgentLastUpdate"}))
	filter, err = Compile(`a:/Hi \d+/`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "Hi 123"}))
}
