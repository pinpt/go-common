package json

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Foo struct {
	Bar string
}

func TestDeserializerSingleArray(t *testing.T) {
	assert := assert.New(t)
	var buf strings.Builder
	foos := make([]Foo, 0)
	foos = append(foos, Foo{"a"})
	foos = append(foos, Foo{"b"})
	foos = append(foos, Foo{"c"})
	foos = append(foos, Foo{"d"})
	foos = append(foos, Foo{"e"})
	b, _ := json.MarshalIndent(foos, "", "\t")
	buf.Write(b)
	var last string
	var count int
	assert.NoError(Deserialize(strings.NewReader(buf.String()), func(line json.RawMessage) error {
		var f Foo
		if err := json.Unmarshal(line, &f); err != nil {
			return err
		}
		count++
		last = f.Bar
		return nil
	}))
	assert.Equal(5, count)
	assert.Equal("e", last)
}

func TestDeserializerSingleObject(t *testing.T) {
	assert := assert.New(t)
	var buf strings.Builder
	b, _ := json.MarshalIndent(Foo{"a"}, "", "\t")
	buf.Write(b)
	var last string
	var count int
	assert.NoError(Deserialize(strings.NewReader(buf.String()), func(line json.RawMessage) error {
		var f Foo
		if err := json.Unmarshal(line, &f); err != nil {
			return err
		}
		count++
		last = f.Bar
		return nil
	}))
	assert.Equal(1, count)
	assert.Equal("a", last)
}

func TestDeserializerSingleObjectCompressed(t *testing.T) {
	assert := assert.New(t)
	var buf strings.Builder
	b, _ := json.Marshal(Foo{"a"})
	buf.Write(b)
	var last string
	var count int
	assert.NoError(Deserialize(strings.NewReader(buf.String()), func(line json.RawMessage) error {
		var f Foo
		if err := json.Unmarshal(line, &f); err != nil {
			return err
		}
		count++
		last = f.Bar
		return nil
	}))
	assert.Equal(1, count)
	assert.Equal("a", last)
}

func TestDeserializerMultipleObjectsCompressed(t *testing.T) {
	assert := assert.New(t)
	var buf strings.Builder
	b, _ := json.Marshal(Foo{"a"})
	buf.Write(b)
	b, _ = json.Marshal(Foo{"b"})
	buf.Write(b)
	b, _ = json.Marshal(Foo{"c"})
	buf.Write(b)
	var last string
	var count int
	assert.NoError(Deserialize(strings.NewReader(buf.String()), func(line json.RawMessage) error {
		var f Foo
		if err := json.Unmarshal(line, &f); err != nil {
			return err
		}
		count++
		last = f.Bar
		return nil
	}))
	assert.Equal(3, count)
	assert.Equal("c", last)
}

func TestDeserializerMultipleObjectsNewLine(t *testing.T) {
	assert := assert.New(t)
	var buf strings.Builder
	b, _ := json.Marshal(Foo{"a"})
	buf.Write(b)
	buf.WriteString("\n")
	b, _ = json.Marshal(Foo{"b"})
	buf.Write(b)
	buf.WriteString("\n")
	b, _ = json.Marshal(Foo{"c"})
	buf.Write(b)
	buf.WriteString("\n")
	var last string
	var count int
	assert.NoError(Deserialize(strings.NewReader(buf.String()), func(line json.RawMessage) error {
		var f Foo
		if err := json.Unmarshal(line, &f); err != nil {
			return err
		}
		count++
		last = f.Bar
		return nil
	}))
	assert.Equal(3, count)
	assert.Equal("c", last)
}

func TestDeserializerConcatenatedStreams(t *testing.T) {
	assert := assert.New(t)
	var buf strings.Builder
	foos := make([]Foo, 0)
	foos = append(foos, Foo{"a"})
	foos = append(foos, Foo{"b"})
	foos = append(foos, Foo{"c"})
	foos = append(foos, Foo{"d"})
	foos = append(foos, Foo{"e"})
	b, _ := json.MarshalIndent(foos, "", "\t")
	buf.Write(b)
	bars := make([]Foo, 0)
	bars = append(bars, Foo{"f"})
	bars = append(bars, Foo{"g"})
	bars = append(bars, Foo{"h"})
	bars = append(bars, Foo{"i"})
	bars = append(bars, Foo{"j"})
	b, _ = json.MarshalIndent(bars, "", "\t")
	buf.Write(b)
	assert.NotEmpty(buf)
	var last string
	var count int
	assert.NoError(Deserialize(strings.NewReader(buf.String()), func(line json.RawMessage) error {
		var f Foo
		if err := json.Unmarshal(line, &f); err != nil {
			return err
		}
		count++
		last = f.Bar
		return nil
	}))
	assert.Equal(10, count)
	assert.Equal("j", last)
}

func TestDeserializerInvalidJSON(t *testing.T) {
	assert := assert.New(t)
	assert.EqualError(Deserialize(strings.NewReader("hi"), func(line json.RawMessage) error {
		return fmt.Errorf("shouldn't have gotten here")
	}), "invalid json, expected either [ or {")
}

func TestDeserializerRef(t *testing.T) {
	assert := assert.New(t)
	var buf strings.Builder
	b, _ := json.Marshal(Foo{"a"})
	buf.Write(b)
	b, _ = json.Marshal(Foo{"b"})
	buf.Write(b)
	arr := make([]json.RawMessage, 0)
	assert.NoError(Deserialize(strings.NewReader(buf.String()), func(line json.RawMessage) error {
		var f Foo
		if err := json.Unmarshal(line, &f); err != nil {
			return err
		}
		arr = append(arr, line)
		return nil
	}))
	assert.Equal("{\"Bar\":\"a\"}", string(arr[0]))
	assert.Equal("{\"Bar\":\"b\"}", string(arr[1]))
}
