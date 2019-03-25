package strings

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInterpolateStrings(t *testing.T) {
	assert := assert.New(t)
	val, err := InterpolateString("abc", nil)
	assert.NoError(err)
	assert.Equal("abc", val)
	val, err = InterpolateString("", nil)
	assert.NoError(err)
	assert.Equal("", val)
	val, err = InterpolateString("this is a {test}", map[string]interface{}{"test": "TEST"})
	assert.NoError(err)
	assert.Equal("this is a TEST", val)
	val, err = InterpolateString("this is a {test} {notfound}", map[string]interface{}{"test": "TEST"})
	assert.NoError(err)
	assert.Equal("this is a TEST {notfound}", val)
	val, err = InterpolateString("this is a {test:-notfound}", map[string]interface{}{"foo": "TEST"})
	assert.NoError(err)
	assert.Equal("this is a notfound", val)
	val, err = InterpolateString("this is a {test:-fail}", map[string]interface{}{"test": "TEST"})
	assert.NoError(err)
	assert.Equal("this is a TEST", val)
	val, err = InterpolateString("this is a {test}", map[string]interface{}{"test": 123})
	assert.NoError(err)
	assert.Equal("this is a 123", val)
	val, err = InterpolateString("this is a {test}", map[string]interface{}{"test": nil})
	assert.NoError(err)
	assert.Equal("this is a {test}", val)
	val, err = InterpolateString("this is a {test}", map[string]interface{}{"test": ""})
	assert.NoError(err)
	assert.Equal("this is a {test}", val)
	val, err = InterpolateString("this is a {!test}", map[string]interface{}{"test": ""})
	assert.EqualError(err, "required value not found for key 'test'")
	assert.Empty(val)
	val, err = InterpolateString("this is a {!test}", map[string]interface{}{"test": nil})
	assert.EqualError(err, "required value not found for key 'test'")
	assert.Empty(val)
}
