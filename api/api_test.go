package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAbsoluteURL(t *testing.T) {
	assert := assert.New(t)
	assert.True(isAbsURL("https://auth-api.pinpt.io/"))
	assert.False(isAbsURL("http://api.edge.api.pinpt.io"))
	assert.False(isAbsURL("http://api.stable.api.pinpt.io"))
	assert.False(isAbsURL("/foo"))
	assert.False(isAbsURL("foo"))
}

func TestBackendURL(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("https://auth-api.edge.pinpt.io/", BackendURL(AuthService, "edge"))
	assert.Equal("https://auth-api.pinpt.io/", BackendURL(AuthService, "stable"))
}

func TestSetHeaders(t *testing.T) {
	assert := assert.New(t)
	req, err := http.NewRequest("GET", BackendURL(AuthService, "edge"), nil)
	assert.NoError(err)
	assert.NotNil(req)
	SetUserAgent(req)
	assert.Equal("Pinpoint Agent/", req.Header.Get("user-agent"))
	SetAuthorization(req, "123")
	assert.Equal("123", req.Header.Get(AuthorizationHeader))
	req.Header.Del(AuthorizationHeader)
	SetAuthorization(req, "")
	assert.Equal("", req.Header.Get(AuthorizationHeader))
}
