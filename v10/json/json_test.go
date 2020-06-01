package json

import (
	"io/ioutil"
	"os"
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

func TestReadFile(t *testing.T) {
	assert := assert.New(t)
	type person struct {
		Name      string `json:"name"`
		HairColor string `json:"hairColor"`
		EyesColor string `json:"eyesColor"`
	}
	tmpfile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(tmpfile.Name())
	ioutil.WriteFile(tmpfile.Name(), []byte(`{
	"name": "Pedro",
	"hairColor": "Dark",
	"eyesColor": "Brown"
}`), 0600)
	var pedro person
	err = ReadFile(tmpfile.Name(), &pedro)
	assert.NoError(err)
	assert.Equal(pedro.Name, "Pedro")
	assert.Equal(pedro.HairColor, "Dark")
	assert.Equal(pedro.EyesColor, "Brown")
}
