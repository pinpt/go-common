package upload

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
