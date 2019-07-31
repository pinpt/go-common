package datamodel

import (
	"testing"

	pjson "github.com/pinpt/go-common/json"
	"github.com/stretchr/testify/assert"
)

func TestEncodeOptionalString(t *testing.T) {
	assert := assert.New(t)
	val, ok := ToGolangObjectNil(true, true)
	assert.True(ok)
	assert.Equal(`{"null":null}`, pjson.Stringify(val))
	val, ok = ToGolangObject(nil, true, true, "string")
	assert.True(ok)
	assert.Equal(`{"null":null}`, pjson.Stringify(val))
	val, ok = ToGolangObject("s", true, true, "string")
	assert.True(ok)
	assert.Equal(`{"string":"s"}`, pjson.Stringify(val))
	s := "s"
	val, ok = ToGolangObject(&s, true, true, "string")
	assert.True(ok)
	assert.Equal(`{"string":"s"}`, pjson.Stringify(val))
	var sp *string
	val, ok = ToGolangObject(sp, true, true, "string")
	assert.True(ok)
	assert.Equal(`{"null":null}`, pjson.Stringify(val))
}
