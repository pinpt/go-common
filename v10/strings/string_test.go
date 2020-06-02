package strings

import (
	"io"
	"io/ioutil"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPointer(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	cad := "test"
	empty := ""
	booleanStr := "true"
	boolean := true
	null := "NULL"
	var ceroInt64 int64
	var ceroInt32 int32
	var ceroInt int

	assert.Equal((*string)(nil), Pointer(nil))

	assert.Equal((*string)(nil), Pointer(empty))
	assert.Equal((*string)(nil), Pointer(null))
	assert.Equal(&cad, Pointer(cad))

	assert.Equal((*string)(nil), Pointer(&empty))
	assert.Equal((*string)(nil), Pointer(&null))
	assert.Equal(&cad, Pointer(&cad))

	assert.Equal((*string)(nil), Pointer(ceroInt64))
	assert.Equal((*string)(nil), Pointer(ceroInt32))
	assert.Equal((*string)(nil), Pointer(ceroInt))

	assert.Equal(&booleanStr, Pointer(boolean))
}

func TestValue(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	cad := "test"
	empty := ""
	boolean := true
	booleanStr := "true"

	var nils *string

	assert.Equal(empty, Value(nil))
	assert.Equal(cad, Value(&cad))
	assert.Equal(booleanStr, Value(boolean))
	assert.Equal("", Value(nils))
}

func TestNewCloseableStringReader(t *testing.T) {
	assert := assert.New(t)

	r := NewCloseableStringReader("hi")
	assert.Implementsf((*io.ReadCloser)(nil), r, "doesn't correct implement the io.ReadCloser interface")
	buf, err := ioutil.ReadAll(r)
	assert.NoError(err)
	assert.Equal("hi", string(buf))
}

func TestNewUUIDV4(t *testing.T) {
	assert := assert.New(t)
	val := NewUUIDV4()
	assert.NotEmpty(val)
	assert.Regexp(regexp.MustCompile(`^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$`), val)
}
