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

// Create a file called test.json, place it in your desktop, and add the following:
// {
// 	"name": "Pedro",
// 	"hairColor": "Dark",
// 	"eyesColor": "Brown"
// }
func TestReadFile(t *testing.T) {
	assert := assert.New(t)
	type person struct {
		Name      string `json:"name"`
		HairColor string `json:"hairColor"`
		EyesColor string `json:"eyesColor"`
	}

	pedro := person{}
	err := ReadFile("/Users/pedro/Desktop/test.json", &pedro)
	assert.NoError(err)
	assert.Equal(pedro.Name, "Pedro")
	assert.Equal(pedro.HairColor, "Dark")
	assert.Equal(pedro.EyesColor, "Brown")
}
