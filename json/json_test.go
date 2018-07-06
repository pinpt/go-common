package json

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSON(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal(`{"a":"b"}`, Stringify(map[string]string{"a": "b"}))

	assert.Equal(`{
	"a": "b"
}`, Stringify(map[string]string{"a": "b"}, true))
}

func TestJSONError(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	assert.Equal(`<error:json: unsupported type: func(io.Reader) io.ReadCloser>`, Stringify(ioutil.NopCloser))
}

func TestStringPointerJSON(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	foo := "foo"
	s := struct {
		A *string `json:"a,omitempty"`
	}{
		A: &foo,
	}
	assert.Equal(`{"a":"foo"}`, Stringify(s))
}
