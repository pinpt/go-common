package upload

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUploadOK(t *testing.T) {
	assert := assert.New(t)
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintln(w, "OK")
	}))
	defer ts.Close()
	parts, size, err := Upload(Options{
		APIKey:      "123",
		Body:        strings.NewReader("hi"),
		ContentType: "text/plain",
		URL:         ts.URL + "/foo.zip",
	})
	assert.NoError(err)
	assert.Equal(int(1), parts)
	assert.Equal(int64(2), size)
}

func TestUploadRetry(t *testing.T) {
	assert := assert.New(t)
	var count int
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count == 0 {
			count++
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintln(w, "OK")
	}))
	defer ts.Close()
	parts, size, err := Upload(Options{
		APIKey:      "123",
		Body:        strings.NewReader("hi"),
		ContentType: "text/plain",
		URL:         ts.URL + "/foo.zip",
	})
	assert.NoError(err)
	assert.Equal(int(1), parts)
	assert.Equal(int64(2), size)
	assert.Equal(1, count)
}

func TestUploadRetryMidRead(t *testing.T) {
	assert := assert.New(t)
	var count int
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count == 0 {
			b := make([]byte, 2)
			c, err := r.Body.Read(b)
			assert.NoError(err)
			assert.Equal(2, c)
			r.Body.Close()
			w.WriteHeader(http.StatusGatewayTimeout)
			count++
			return
		}
		io.Copy(ioutil.Discard, r.Body)
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintln(w, "OK")
	}))
	defer ts.Close()
	parts, size, err := Upload(Options{
		APIKey:      "123",
		Body:        strings.NewReader("1234567890"),
		ContentType: "text/plain",
		URL:         ts.URL + "/foo.zip",
	})
	assert.NoError(err)
	assert.Equal(int(1), parts)
	assert.Equal(int64(10), size)
	assert.Equal(1, count)
}

func TestUploadTimeout(t *testing.T) {
	assert := assert.New(t)
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusGatewayTimeout)
	}))
	defer ts.Close()
	parts, size, err := Upload(Options{
		APIKey:      "123",
		Body:        strings.NewReader("1234567890"),
		ContentType: "text/plain",
		URL:         ts.URL + "/foo.zip",
		Deadline:    time.Now().Add(time.Second * 2),
	})
	assert.EqualError(err, ErrDeadlineReached.Error())
	assert.Equal(int(0), parts)
	assert.Equal(int64(0), size)
}

func TestUploadSplitPartsMinimum(t *testing.T) {
	assert := assert.New(t)
	var count int
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()
	parts, size, err := Upload(Options{
		PartSize:    5,
		APIKey:      "123",
		Body:        strings.NewReader("1234567890"),
		ContentType: "text/plain",
		URL:         ts.URL + "/foo.zip",
		Deadline:    time.Now().Add(time.Second * 2),
	})
	assert.NoError(err)
	assert.Equal(int(1), parts)
	assert.Equal(int64(10), size)
	assert.Equal(1, count)
}

func TestUploadSplitPartsOver(t *testing.T) {
	assert := assert.New(t)
	var count int32
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()
	buf := make([]byte, MinUploadPartSize*2)
	for i := 0; i < len(buf); i++ {
		buf[i] = 0xf
	}
	parts, size, err := Upload(Options{
		APIKey:      "123",
		Body:        bytes.NewReader(buf),
		ContentType: "text/plain",
		URL:         ts.URL + "/foo.zip",
		Deadline:    time.Now().Add(time.Second * 2),
	})
	assert.NoError(err)
	assert.Equal(int(2), parts)
	assert.Equal(int64(len(buf)), size)
	assert.Equal(int32(2), atomic.LoadInt32(&count))
}
