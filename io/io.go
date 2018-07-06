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
	var out io.WriteCloser
	out = f
	if filepath.Ext(fn) == ".gz" {
		out, _ = gzip.NewWriterLevel(f, gzip.BestCompression)
	}
	return out, nil
}

type JSONStream struct {
	stream io.WriteCloser
	enc    *json.Encoder
}

// Write will stream a JSON line to the output stream
func (s *JSONStream) Write(obj interface{}) error {
	return s.enc.Encode(obj)
}

// Close will close the stream
func (s *JSONStream) Close() error {
	return s.stream.Close()
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
	}
	return stream, nil
}
