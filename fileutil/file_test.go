package fileutil

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"os"
	"path/filepath"
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

func TestZipDir(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir("", "pinpttest")
	assert.NoError(err)
	defer os.Remove(dir)
	f1 := filepath.Join(dir, "include.txt")
	f2 := filepath.Join(dir, "include2.txt")
	f3 := filepath.Join(dir, "exclude.txt")
	assert.NoError(ioutil.WriteFile(f1, []byte("1"), 0644))
	assert.NoError(ioutil.WriteFile(f2, []byte("2"), 0644))
	assert.NoError(ioutil.WriteFile(f3, []byte("foo"), 0644))
	defer os.Remove(f1)
	defer os.Remove(f2)
	defer os.Remove(f3)
	outdir, err := ioutil.TempDir("", "pinpttest")
	assert.NoError(err)
	defer os.Remove(outdir)
	output := filepath.Join(outdir, "test.zip")
	count, err := ZipDir(output, dir, regexp.MustCompile("include"))
	assert.NoError(err)
	defer os.Remove(output)

	assert.EqualValues(2, count)
	reader, err := zip.OpenReader(output)
	assert.NoError(err)
	if len(reader.File) != 2 {
		assert.Fail("zip was empty")
	}
	for _, f := range reader.File {
		if f.Name == "include.txt" {
			r, err := f.Open()
			assert.NoError(err)
			data, err := ioutil.ReadAll(r)
			assert.NoError(err)
			r.Close()
			assert.EqualValues("1", data)
		} else if f.Name == "include2.txt" {
			r, err := f.Open()
			assert.NoError(err)
			data, err := ioutil.ReadAll(r)
			assert.NoError(err)
			r.Close()
			assert.EqualValues("2", data)
		} else {
			assert.Fail("zip archive contained unexpected file: %s", f.Name)
		}
	}
}

func TestOpenNestedZip(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir("", "pinpttest")
	assert.NoError(err)
	defer os.Remove(dir)
	// create nested zip
	f1 := filepath.Join(dir, "include.txt")
	f2 := filepath.Join(dir, "include2.txt")
	assert.NoError(ioutil.WriteFile(f1, []byte("1"), 0644))
	assert.NoError(ioutil.WriteFile(f2, []byte("2"), 0644))
	defer os.Remove(f1)
	defer os.Remove(f2)
	outdir, err := ioutil.TempDir("", "pinpttest")
	assert.NoError(err)
	defer os.Remove(outdir)
	nested := filepath.Join(outdir, "inside.zip")
	count, err := ZipDir(nested, dir, regexp.MustCompile("include"))
	assert.NoError(err)
	assert.EqualValues(2, count)
	defer os.Remove(nested)

	output := filepath.Join(outdir, "outside.zip")
	count, err = ZipDir(output, outdir, regexp.MustCompile("inside.zip"))
	assert.NoError(err)
	assert.EqualValues(1, count)
	defer os.Remove(output)

	reader, err := OpenNestedZip(output, "inside.zip")
	assert.NoError(err)
	if len(reader.File) != 2 {
		assert.Fail("zip was empty")
	}
	for _, f := range reader.File {
		if f.Name == "include.txt" {
			r, err := f.Open()
			assert.NoError(err)
			data, err := ioutil.ReadAll(r)
			assert.NoError(err)
			r.Close()
			assert.EqualValues("1", data)
		} else if f.Name == "include2.txt" {
			r, err := f.Open()
			assert.NoError(err)
			data, err := ioutil.ReadAll(r)
			assert.NoError(err)
			r.Close()
			assert.EqualValues("2", data)
		} else {
			assert.Fail("zip archive contained unexpected file: %s", f.Name)
		}
	}
}
