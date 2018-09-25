package api

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAbsoluteURL(t *testing.T) {
	assert := assert.New(t)
	assert.True(isAbsURL("https://pinpoint.pinpt.io/api/"))
	assert.True(isAbsURL("https://pinpt.io/api/"))
	assert.False(isAbsURL("http://api.edge.api.pinpt.io"))
	assert.False(isAbsURL("http://api.stable.api.pinpt.io"))
	assert.False(isAbsURL("/foo"))
	assert.False(isAbsURL("foo"))
}

func TestBackendURL(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("https://foo.edge.pinpt.io/api/", BackendURL("foo", "edge"))
	assert.Equal("https://foo.pinpt.io/api/", BackendURL("foo", "stable"))
}

func TestBackendURLPing(t *testing.T) {
	assert := assert.New(t)
	resp, err := Get(context.Background(), "api", "edge", "/frontend/ping", "")
	assert.NoError(err)
	buf, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	assert.NoError(err)
	assert.Equal("OK", string(buf))

	resp, err = Get(context.Background(), "api", "stable", "/frontend/ping", "")
	assert.NoError(err)
	buf, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	assert.NoError(err)
	assert.Equal("OK", string(buf))
}

func TestDomainName(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("foobar", getCustomerSubdomain("foo bar"))
	assert.Equal("foobar", getCustomerSubdomain("foo\tbar"))
	assert.Equal("foobar", getCustomerSubdomain("foo+bar"))
	assert.Equal("foobar", getCustomerSubdomain("foo,bar"))
	assert.Equal("foobar", getCustomerSubdomain("foo.bar"))
	assert.Equal("foobar", getCustomerSubdomain("foobar"))
	assert.Equal("foobarinc", getCustomerSubdomain("Foo Bar, Inc."))
}
