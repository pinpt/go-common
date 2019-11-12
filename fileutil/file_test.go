package fileutil

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolve(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	ok, error := Resolve("")
	assert.Error(error)
	assert.Empty(ok)

	ok, error = Resolve("file.go")
	assert.Nil(error)
	assert.NotNil(ok)
}

func TestFileExists(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	ok := FileExists("../uti0l/", "file.go")

	assert.Equal(false, ok)
	assert.Equal(false, FileExists())
}

func TestResolveFileName(t *testing.T) {
	// t.Parallel()
	assert := assert.New(t)

	str := "file.go"
	assert.Equal(str, ResolveFileName(str))

	str = "temp"
	os.Create(str + ".gz")
	assert.Equal(str+".gz", ResolveFileName(str))
	os.Remove(str + ".gz")
}

func TestOpenFile(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	ok, err := OpenFile("file.go")
	assert.Nil(err)
	assert.NotNil(ok)
	defer ok.Close()

	ok, err = OpenFile("file2.go")
	assert.Error(err)
	assert.Nil(ok)

	os.Create("file.gz")
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write([]byte("hello, world\n"))
	w.Close()
	err = ioutil.WriteFile("file.gz", b.Bytes(), 0666)
	assert.NoError(err)
	ok, err = OpenFile("file.gz")
	assert.Nil(err)
	assert.NotNil(ok)
	os.Remove("file.gz")
	defer ok.Close()
}

func TestFindFiles(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	os.Mkdir("temp", os.FileMode(0700))
	os.Create("temp/temp1.tmp")

	ok, err := FindFiles("temp", regexp.MustCompile(".tmp"))
	assert.Nil(err)
	assert.Equal([]string{"temp/temp1.tmp"}, ok)
	os.Remove("temp/temp1.tmp")
	os.Remove("temp")
}

func TestIsZeroLengthFile(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	os.Create("temp.tmp")
	assert.Equal(true, IsZeroLengthFile("temp.tmp"))
	os.Remove("temp.tmp")

	assert.Equal(true, IsZeroLengthFile("temp2.tmp"))
}
