package io

import (
	"compress/gzip"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

// Checks that NewStream with .gz does not leak open files. This returns too many open files error if NewStream is not using writerWrapper
func TestStreamGzipNoLeaks(t *testing.T) {
	var streams []io.WriteCloser
	for i := 0; i < 1000; i++ {

		d, _ := ioutil.TempDir("", "")
		fn := filepath.Join(d, "temp.gz")
		stream, err := NewStream(fn)
		if err != nil {
			t.Fatal(err)
		}
		_, err = stream.Write([]byte("test"))
		if err != nil {
			t.Fatal(err)
		}
		err = stream.Close()
		if err != nil {
			t.Fatal(err)
		}

		// to avoid garbage collection, we want to see if the file is closed even if we keep a reference to io.WriteCloser after closing
		streams = append(streams, stream)

		f, err := os.Open(fn)
		if err != nil {
			t.Fatal(err)
		}
		gr, err := gzip.NewReader(f)
		if err != nil {
			t.Fatal(err)
		}
		b, err := ioutil.ReadAll(gr)
		if err != nil {
			t.Fatal(err)
		}

		if string(b) != "test" {
			t.Fatal("resulting file content is not correct")
		}

		f.Close()
	}
}
