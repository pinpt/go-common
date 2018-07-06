package strings

import (
	"fmt"
	"io"
	"strings"
)

// Pointer will return a pointer to a string handling empty string as nil
func Pointer(v interface{}) *string {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok {
		if s == "" || s == "NULL" {
			return nil
		}
		return &s
	}
	if s, ok := v.(*string); ok {
		if *s == "" || *s == "NULL" {
			return nil
		}
		return s
	}
	if i, ok := v.(int); ok {
		if i == 0 {
			return nil
		}
	}
	if i, ok := v.(int32); ok {
		if i == 0 {
			return nil
		}
	}
	if i, ok := v.(int64); ok {
		if i == 0 {
			return nil
		}
	}
	return Pointer(fmt.Sprintf("%v", v))
}

// Value returns a string value for an interface or empty string if nil
func Value(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(*string); ok {
		if s == nil {
			return ""
		}
		return *s
	}
	return fmt.Sprintf("%v", v)
}

// CloseableStringReader is a string reader which implements the io.ReadCloser interface
type CloseableStringReader struct {
	reader io.Reader
}

// Read will reader from buffer
func (r *CloseableStringReader) Read(b []byte) (n int, err error) {
	return r.reader.Read(b)
}

// Close will do nothing
func (r *CloseableStringReader) Close() error {
	return nil
}

// NewCloseableStringReader will return a closeable reader
func NewCloseableStringReader(input string) *CloseableStringReader {
	return &CloseableStringReader{strings.NewReader(input)}
}
