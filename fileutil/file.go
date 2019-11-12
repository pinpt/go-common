package fileutil

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Resolve with resolve a relative file path
func Resolve(filename string) (string, error) {
	if filename == "" {
		return filename, fmt.Errorf("filename was not supplied")
	}
	f, err := filepath.Abs(filename)
	if err != nil {
		return filename, err
	}
	_, err = os.Stat(f)
	if os.IsNotExist(err) {
		return f, err
	}
	return f, err
}

// FileExists returns true if the path components exist
func FileExists(filename ...string) bool {
	fn := filepath.Join(filename...)
	_, err := Resolve(fn)
	if err != nil {
		return false
	}
	return true
}

// ResolveFileName will attempt to resolve a filename attempting to look at both the basic name or the name + .gz extension
func ResolveFileName(fn string) string {
	if !FileExists(fn) {
		nfn := fn + ".gz"
		if FileExists(nfn) {
			return nfn
		}
	}
	return fn
}

type combinedReader struct {
	f io.ReadCloser
	g *gzip.Reader
}

func (c *combinedReader) Read(p []byte) (int, error) {
	return c.g.Read(p)
}

func (c *combinedReader) Close() error {
	c.g.Close()
	return c.f.Close()
}

// OpenFile will open a file and if it's gzipped return a gzipped reader as io.ReadCloser but will close both streams on close
func OpenFile(fn string) (io.ReadCloser, error) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	var result io.ReadCloser = f
	if filepath.Ext(fn) == ".gz" {
		r, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil, err
		}
		result = &combinedReader{f, r}
	}
	return result, nil
}

// FindFiles helper
func FindFiles(dir string, pattern *regexp.Regexp) ([]string, error) {
	fileList := []string{}
	err := filepath.Walk(dir, func(p string, f os.FileInfo, err error) error {
		// fmt.Println("trying to match", pattern, "for", path)
		if pattern.MatchString(filepath.Base(p)) && !strings.Contains(p, "gitdata") {
			fileList = append(fileList, p)
		}
		return nil
	})
	return fileList, err
}

// IsZeroLengthFile returns true if the filename specified is empty (0 bytes)
func IsZeroLengthFile(fn string) bool {
	f, err := os.Open(fn)
	defer f.Close()
	if err != nil {
		return true
	}
	s, err := f.Stat()
	if err != nil {
		return true
	}
	return s.Size() == 0
}
