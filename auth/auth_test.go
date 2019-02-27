package auth

import (
	"fmt"
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
	assert.Equal("hello world", dec1)

	// s%3An62lbKqtNibwptxyy4oN8UB4cq2tFVzgwery2EHCnOtMDGFA%252BBYd%252FuKTMPOm9ZMuWOrZzQMckY9FkWLiFF%252BdBfuEZXrZ8vNHEcwOC96Yp1SNIT8vKQLBfdypEojWxJhw7jMb1QeGgCkFfPxV7CDlqXZV2Hthwz%252F3Tb08Sa%252BYggpOXw%253D%253D.Pn0%2FRF59XxTEZBPlFodTCEhgEDBl%2FU0jgdsMrHsDKo4
	// s%3AhIcRXxzgFt9%252F9jYWmRqfc%252FZ3EXuz4cBISvTrGUDHoeyDzkLf4bJmqPxD68G7ocjwIPI4Tsb6d4oMKEPbH3c4653IsKI40aWFM%252Bwicp4BKCuD%252FogNkE%252BGVNHZiNMY42GUWLqY4fMjlANnyCT4occHG8x492a8IVT8aBkqXTmQVzNPMQ%253D%253D.PwC2MAiUYxpnqCe774lCNvZDQO2BamRxI33B4ox2JuE
	esc, _ := url.PathUnescape("4sqHeZZYfHLOLjQC441U7%2F2sbZKrN3Nm13EpBT7F5GqDcuYv4mZSqDu1FqQ3kefMXEQzqr14RsO9J%2FwIPFyLK3pV7iT%2BRs047dGaW%2Frhb5MDhMpgZxhBNCtFbmN80Ihs9EaYfWKWQp7T1RhM8acGXkk1IiywYmQ2qomc3rpQgaKq%2Fw%3D%3D")
	assert.Equal("P+m32ewamHkyE4JgDNGnNW18w/dQ/lKPxoQdprGCwF/SH0OTi/EiUF16sIeLOMtULJCDbwzSNL8mf+t/Nn4XoPN9Eu1Jj/ujijSWStXG3lZfg3FnisVc2rlDaHkFAQrZGpRlgxVOOvG18vq7TOHP1sTQZ6E9B7C6wsY05Q6gnajkLg==", esc)
	fmt.Println(esc)
	dec2, err := DecryptString(esc, "^LB-f83tk^ceL866H2zj$M$jgvP3XHtc")
	assert.NoError(err)
	assert.Equal("{\"customer_id\":\"5500a5ba8135f296\",\"scopes\":[],\"expiry\":1547756915352,\"type\":\"session\"}", dec2)
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
	assert.Equal("", enc)
	dec, err := DecryptString(enc, "^LB-f83tk^ceL866H2zj$M$jgvP3XHtc")
	assert.NoError(err)
	assert.NotEmpty(dec)
}
