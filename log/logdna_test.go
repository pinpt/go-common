package log

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLogDNA(t *testing.T) {
	assert := assert.New(t)
	os.Setenv("PP_HOSTNAME", "foobar")
	os.Setenv("PP_LOG_KEY", "123")
	globalClient = nil
	dnalogger := newDNALogger(nil)
	l := dnalogger.(*dnalog)
	assert.NotNil(l)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		buf, err := ioutil.ReadAll(req.Body)
		assert.NoError(err)
		var p payload
		assert.NoError(json.Unmarshal(buf, &p))
		assert.Len(p.Lines, 1)
		assert.Equal("hi", p.Lines[0].Line)
		assert.Equal("info", p.Lines[0].Level)
		assert.NotEmpty(p.Lines[0].Timestamp)
		assert.Empty(p.Lines[0].Meta)
		w.WriteHeader(200)
	}))
	defer server.Close()
	l.client.url = server.URL
	dnalogger.Log("msg", "hi")
	assert.NoError(dnalogger.Close())
}

func TestNewLogDNAWithLevel(t *testing.T) {
	assert := assert.New(t)
	os.Setenv("PP_HOSTNAME", "foobar")
	os.Setenv("PP_LOG_KEY", "123")
	globalClient = nil
	dnalogger := newDNALogger(nil)
	l := dnalogger.(*dnalog)
	assert.NotNil(l)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		buf, err := ioutil.ReadAll(req.Body)
		assert.NoError(err)
		var p payload
		assert.NoError(json.Unmarshal(buf, &p))
		assert.Len(p.Lines, 1)
		assert.Equal("hi", p.Lines[0].Line)
		assert.Equal("debug", p.Lines[0].Level)
		assert.NotEmpty(p.Lines[0].Timestamp)
		assert.Empty(p.Lines[0].Meta)
		w.WriteHeader(200)
	}))
	defer server.Close()
	l.client.url = server.URL
	Debug(dnalogger, "hi")
	assert.NoError(dnalogger.Close())
}

func TestNewLogDNAWithMetadata(t *testing.T) {
	assert := assert.New(t)
	os.Setenv("PP_HOSTNAME", "foobar")
	os.Setenv("PP_LOG_KEY", "123")
	globalClient = nil
	dnalogger := newDNALogger(nil)
	l := dnalogger.(*dnalog)
	assert.NotNil(l)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		buf, err := ioutil.ReadAll(req.Body)
		assert.NoError(err)
		var p payload
		assert.NoError(json.Unmarshal(buf, &p))
		assert.Len(p.Lines, 1)
		assert.Equal("hi", p.Lines[0].Line)
		assert.Equal("debug", p.Lines[0].Level)
		assert.NotEmpty(p.Lines[0].Timestamp)
		assert.NotEmpty(p.Lines[0].Meta)
		assert.Equal("b", p.Lines[0].Meta["a"])
		w.WriteHeader(200)
	}))
	defer server.Close()
	l.client.url = server.URL
	Debug(dnalogger, "hi", "a", "b")
	assert.NoError(dnalogger.Close())
}

func TestNewLogDNAWithMasking(t *testing.T) {
	assert := assert.New(t)
	os.Setenv("PP_HOSTNAME", "foo")
	os.Setenv("PP_LOG_KEY", "123")
	os.Setenv("PP_LOG_TAGS", "")
	globalClient = nil
	var wg sync.WaitGroup
	wg.Add(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer wg.Done()
		buf, err := ioutil.ReadAll(req.Body)
		assert.NoError(err)
		var p payload
		assert.NoError(json.Unmarshal(buf, &p))
		assert.Len(p.Lines, 1)
		assert.Equal("hi", p.Lines[0].Line)
		assert.Equal("info", p.Lines[0].Level)
		assert.NotEmpty(p.Lines[0].Timestamp)
		assert.NotEmpty(p.Lines[0].Meta)
		assert.Equal("super******", p.Lines[0].Meta["password"])
		w.WriteHeader(200)
	}))
	os.Setenv("PP_LOG_URL", server.URL)
	var w strings.Builder
	dnalogger := NewLogger(&w, JSONLogFormat, NoColorTheme, DebugLevel, "foo")
	defer server.Close()
	Info(dnalogger, "hi", "password", "supersecret")
	assert.NoError(dnalogger.Close())
	assert.Equal(`{"level":"info","msg":"hi","password":"super******","pkg":"foo"}`, strings.TrimSpace(w.String()))
	wg.Wait()
}

func TestNewLogDNAWithTags(t *testing.T) {
	assert := assert.New(t)
	os.Setenv("PP_HOSTNAME", "foo")
	os.Setenv("PP_LOG_KEY", "123")
	os.Setenv("PP_LOG_TAGS", "1,2,3")
	globalClient = nil
	var wg sync.WaitGroup
	wg.Add(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer wg.Done()
		assert.True(strings.Contains(req.URL.String(), "tags=1%2C2%2C3"))
		buf, err := ioutil.ReadAll(req.Body)
		assert.NoError(err)
		var p payload
		assert.NoError(json.Unmarshal(buf, &p))
		assert.Len(p.Lines, 1)
		assert.Equal("hi", p.Lines[0].Line)
		assert.Equal("info", p.Lines[0].Level)
		assert.NotEmpty(p.Lines[0].Timestamp)
		assert.NotEmpty(p.Lines[0].Meta)
		assert.Equal("super******", p.Lines[0].Meta["password"])
		w.WriteHeader(200)
	}))
	os.Setenv("PP_LOG_URL", server.URL)
	var w strings.Builder
	dnalogger := NewLogger(&w, JSONLogFormat, NoColorTheme, DebugLevel, "foo")
	defer server.Close()
	Info(dnalogger, "hi", "password", "supersecret")
	assert.NoError(dnalogger.Close())
	assert.Equal(`{"level":"info","msg":"hi","password":"super******","pkg":"foo"}`, strings.TrimSpace(w.String()))
	wg.Wait()
}

func TestNewLogDNAWithHostnameNotProvided(t *testing.T) {
	assert := assert.New(t)
	os.Setenv("PP_HOSTNAME", "")
	os.Setenv("PP_LOG_KEY", "123")
	os.Setenv("PP_LOG_TAGS", "")
	globalClient = nil
	var wg sync.WaitGroup
	wg.Add(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer wg.Done()
		assert.True(strings.Contains(req.URL.String(), "hostname.not.provided"))
		buf, err := ioutil.ReadAll(req.Body)
		assert.NoError(err)
		var p payload
		assert.NoError(json.Unmarshal(buf, &p))
		assert.Len(p.Lines, 1)
		assert.Equal("hi", p.Lines[0].Line)
		assert.Equal("info", p.Lines[0].Level)
		assert.NotEmpty(p.Lines[0].Timestamp)
		assert.NotEmpty(p.Lines[0].Meta)
		assert.Equal("super******", p.Lines[0].Meta["password"])
		w.WriteHeader(200)
	}))
	os.Setenv("PP_LOG_URL", server.URL)
	var w strings.Builder
	dnalogger := NewLogger(&w, JSONLogFormat, NoColorTheme, DebugLevel, "foo")
	defer server.Close()
	Info(dnalogger, "hi", "password", "supersecret")
	assert.NoError(dnalogger.Close())
	assert.Equal(`{"level":"info","msg":"hi","password":"super******","pkg":"foo"}`, strings.TrimSpace(w.String()))
	wg.Wait()
}
