package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemoryState(t *testing.T) {
	assert := assert.New(t)

	var state State
	state = NewInMemoryState()
	assert.NotNil(state)

	state.Set("foo", "bar")
	val, ok, _ := state.Get("foo")
	assert.True(ok)
	assert.Equal(val, "bar")

	val = state.MustGet("foo")
	assert.Equal(val, "bar")

	assert.Panics(func() {
		state.MustGet("foos")
	})

	val, ok, _ = state.GetOrSet("foofoo", func() interface{} {
		return "barbar"
	})
	assert.False(ok)
	assert.Equal(val, "barbar")

	state.Del("foo")
	val, ok, _ = state.Get("foo")
	assert.False(ok)
	assert.Equal(val, nil)

	err := state.Range(func(key string, val interface{}, kv map[string]interface{}) error {
		assert.Equal(key, "foofoo")
		assert.Equal(val, "barbar")
		assert.EqualValues(map[string]interface{}{"foofoo": "barbar"}, kv)
		return nil
	})
	assert.NoError(err)

	err = state.Invoke("foofoo", func(val interface{}, kv map[string]interface{}) error {
		assert.Equal(val, "barbar")
		assert.EqualValues(map[string]interface{}{"foofoo": "barbar"}, kv)
		return nil
	})
	assert.NoError(err)

	assert.Equal(state.String(), `{"foofoo":"barbar"}`)
}
