package io

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// NewStream returns a new output stream. If the filename extension ends with .gz, it will
// return a gzip output stream
func NewStream(fn string) (io.WriteCloser, error) {
	f, err := os.Create(fn)
	if err != nil {
		return nil, fmt.Errorf("error creating %v. %v", fn, err)
	}
	if filepath.Ext(fn) != ".gz" {
		return f, nil
	}
	gz, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		return nil, err
	}
	wrapper := &writerWrapper{
		original: f,
		wrapper:  gz,
	}
	return wrapper, nil
}

type writerWrapper struct {
	original io.WriteCloser
	wrapper  *gzip.Writer
}

func (s *writerWrapper) Write(p []byte) (n int, _ error) {
	return s.wrapper.Write(p)
}

func (s *writerWrapper) Close() error {
	if err := s.wrapper.Flush(); err != nil {
		return fmt.Errorf("error flushing gzip stream: %v", err)
	}
	if err := s.wrapper.Close(); err != nil {
		return err
	}
	return s.original.Close()
}

// JSONStream is a convenience class for streaming json objects to a file (one JSON per line)
type JSONStream struct {
	stream io.WriteCloser
	enc    *json.Encoder
	name   string
}

// Write will stream a JSON line to the output stream
func (s *JSONStream) Write(obj interface{}) error {
	return s.enc.Encode(obj)
}

// Close will close the stream
func (s *JSONStream) Close() error {
	return s.stream.Close()
}

// Name returns the underlying filename for the stream
func (s *JSONStream) Name() string {
	return s.name
}

// NewJSONStream will return a JSON stream encoder
func NewJSONStream(fn string) (*JSONStream, error) {
	out, err := NewStream(fn)
	if err != nil {
		return nil, err
	}
	stream := &JSONStream{
		stream: out,
		enc:    json.NewEncoder(out),
		name:   fn,
	}
	return stream, nil
}
