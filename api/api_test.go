package api

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAbsoluteURL(t *testing.T) {
	assert := assert.New(t)
	assert.True(isAbsURL("https://api.edge.api.pinpt.io"))
	assert.True(isAbsURL("https://api.stable.api.pinpt.io"))
	assert.False(isAbsURL("http://api.edge.api.pinpt.io"))
	assert.False(isAbsURL("http://api.stable.api.pinpt.io"))
	assert.False(isAbsURL("/foo"))
	assert.False(isAbsURL("foo"))
}

func TestBackendURL(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("https://api.edge.api.pinpt.io", BackendURL("edge"))
	assert.Equal("https://api.stable.api.pinpt.io", BackendURL("stable"))
}

func TestBackendURLPing(t *testing.T) {
	assert := assert.New(t)
	resp, err := Get(context.Background(), "edge", "/frontend/ping", "")
	assert.NoError(err)
	buf, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	assert.NoError(err)
	assert.Equal("OK", string(buf))

	resp, err = Get(context.Background(), "stable", "/frontend/ping", "")
	assert.NoError(err)
	buf, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	assert.NoError(err)
	assert.Equal("OK", string(buf))
}
