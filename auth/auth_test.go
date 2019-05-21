package auth

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testKey = "^LB-f83tk^c9L866H20j$M$jxvP3Xxtc"

func TestAuth(t *testing.T) {
	assert := assert.New(t)
	buf, err := EncryptString("hello world", testKey)
	assert.NoError(err)
	dec, err := DecryptString(buf, testKey)
	assert.NoError(err)
	assert.Equal("hello world", dec)
	buf, err = EncryptString("XCbJbjd72q", "DTY62mV2Cv")
	assert.NoError(err)
	dec, err = DecryptString(buf, "DTY62mV2Cv")
	assert.NoError(err)
	assert.Equal("XCbJbjd72q", dec)

	// js encrypted string
	dec1, err := DecryptString("OxaqwaFR7XFwlvPQmqz6HZa14h0oIlXl2z48HemVnkFL12gWaur+WbhgoGGZeuIa8Cd6vQgi3g==", testKey)
	assert.NoError(err)
	assert.Equal("hello world", dec1)
}

func TestAuthInvalid(t *testing.T) {
	assert := assert.New(t)
	buf, err := EncryptString("hello world", testKey)
	assert.NoError(err)
	dec, err := DecryptString(buf, "12xx")
	assert.EqualError(err, "cipher: message authentication failed")
	assert.Empty(dec)
}

func TestCookieKeyValue(t *testing.T) {
	assert := assert.New(t)
	enc, _ := url.PathUnescape("s%3AlqGI4NztuaWSoaucE2R8ltP%2BPkQl4asO431bdwSZPKOBnfPE%2FjdklX3xb2EQ0pYLdD387fMEt7l8XDRQ%2BUiSONirXIijNuMSHMH1Y1eJsvJzBKSmnpLOM%2FwK6%2FBvRUOgoLhWjK8ptQJCzW337PDq%2BSVSGXa0K6PoOrqqFzfT5jr8Ow%3D%3D.OBTD2hISlJZYYtfey8YQH8Y6YPcz0xFUPA%2FaQBT1q5M")
	indx1 := strings.Index(enc, ":") + 1
	indx2 := strings.Index(enc, ".")
	enc = enc[indx1:indx2]
	assert.Equal("lqGI4NztuaWSoaucE2R8ltP+PkQl4asO431bdwSZPKOBnfPE/jdklX3xb2EQ0pYLdD387fMEt7l8XDRQ+UiSONirXIijNuMSHMH1Y1eJsvJzBKSmnpLOM/wK6/BvRUOgoLhWjK8ptQJCzW337PDq+SVSGXa0K6PoOrqqFzfT5jr8Ow==", enc)
	dec, err := DecryptString(enc, "^LB-f83tk^ceL866H2zj$M$jgvP3XHtc")
	assert.NoError(err)
	assert.NotEmpty(dec)
}
