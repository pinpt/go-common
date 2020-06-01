package httpdefaults

import (
	"net"
	"net/http"
	"time"
)

// DefaultTransport returns a http.Transport with default values set similar to http.DefaultTransport
//
// It establishes network connections as needed and caches them for reuse by subsequent calls.
//
// Do not create a new copy for each request, as this would lead to connection and memory leaks.
func DefaultTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}
