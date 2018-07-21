package api

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/pinpt/go-common/httpdefaults"
	"github.com/pinpt/go-common/json"
	"github.com/pinpt/httpclient"
)

// baseURL is the url to the Pinpoint Cloud API
const baseURL = "api.pinpt.io"

var (
	// domainRegex are the only domains that we trust
	domainRegex = regexp.MustCompile("\\.(pinpt|ppoint)\\.(net|com|io|co)$")

	// caWeTrust are the only Certificate Authorities we trust for our certificate
	// see list from Amazon AwS at https://www.amazontrust.com/repository/
	caWeTrust = map[string]bool{
		"fbe3018031f9586bcbf41727e417b7d1c45c2f47f93be372a17b96b50757d5a2": true, //CN=Amazon Root CA 1,O=Amazon,C=US
		"7f4296fc5b6a4e3b35d3c369623e364ab1af381d8fa7121533c9d6c633ea2461": true, //CN=Amazon Root CA 2,O=Amazon,C=US
		"36abc32656acfc645c61b71613c4bf21c787f5cabbee48348d58597803d7abc9": true, //CN=Amazon Root CA 3,O=Amazon,C=US
		"f7ecded5c66047d28ed6466b543c40e0743abe81d109254dcf845d4c2c7853c5": true, //CN=Amazon Root CA 4,O=Amazon,C=US
		"b58539ecaa13921ccdb80d38d34875fde6471c5a159d9beef2fa6f99983bd611": true, //CN=Amazon,OU=Server CA 0A,O=Amazon,C=US
		"64bb5bd80311fa3f53bd99404cab8762ad6e048447d0a97f219fceca1661f37c": true, //CN=Amazon,OU=Server CA 1A,O=Amazon,C=US
		"1bda5afff83781380bf708198f9f7d2db1e067f14bb9ea7bae70aac0b0305e7b": true, //CN=Amazon,OU=Server CA 2A,O=Amazon,C=US
		"7c53b5df79135d9af6195ef6bb73ab98c2c89950b892b192dffd784db925a41c": true, //CN=Amazon,OU=Server CA 3A,O=Amazon,C=US
		"17708dff2b7faec9cb1b5215ebb2421d97b0543c936fac9d6e02b92f20e5c707": true, //CN=Amazon,OU=Server CA 4A,O=Amazon,C=US
	}
)

// isTrusted returns true if the Certificate is a CA Certificate that we explicitly trust
func isTrusted(cert *x509.Certificate) bool {
	der, _ := x509.MarshalPKIXPublicKey(cert.PublicKey)
	hash := sha256.Sum256(der)
	fingerprint := fmt.Sprintf("%x", hash)
	return caWeTrust[fingerprint]
}

// isDNSNameTrusted returns true if the DNS name is a domain we explicitly trust
func isDNSNameTrusted(names ...string) bool {
	for _, name := range names {
		if domainRegex.MatchString(name) {
			return true
		}
	}
	return false
}

// BackendURL return the base url to the API server
func BackendURL(channel string) string {
	if channel == "" {
		channel = "stable"
	}
	return fmt.Sprintf("https://api.%s.%s", channel, baseURL)
}

// NewHTTPAPIClient will return a new HTTP client for talking with the Pinpoint API
// it will only allow a trusted TLS connection with a valid TLS certificate signed
// by a trusted Certificate Authority and for a DNS name that is owned by Pinpoint
func NewHTTPAPIClient(config *httpclient.Config) (httpclient.Client, error) {
	dial := func(network, addr string) (net.Conn, error) {
		config := &tls.Config{
			InsecureSkipVerify: false,
		}
		conn, err := tls.Dial(network, addr, config)
		if err != nil {
			return nil, err
		}
		state := conn.ConnectionState()
		var certtrusted, catrusted bool
		for _, cert := range state.PeerCertificates {
			if !cert.IsCA {
				if isDNSNameTrusted(cert.DNSNames...) {
					certtrusted = true
				}
			}
			if cert.IsCA && isTrusted(cert) {
				catrusted = true
				// cert comes before CA so at this point we can stop
				break
			}
		}
		if certtrusted && catrusted {
			return conn, nil
		}
		// close up shop, not valid
		conn.Close()
		return nil, fmt.Errorf("invalid TLS certificate. expected a valid Certificate signed by a trusted Pinpoint Certificate Authority and well known domain")
	}
	t := httpdefaults.DefaultTransport()
	t.DialTLS = dial
	client := &http.Client{
		Transport: t,
	}
	if config == nil {
		config = httpclient.NewConfig()
	}
	hc := httpclient.NewHTTPClient(context.Background(), config, client)
	return hc, nil
}

// NewHTTPAPIClientDefault will return a new HTTP client for talking with the Pinpoint API
// it will only allow a trusted TLS connection with a valid TLS certificate signed
// by a trusted Certificate Authority and for a DNS name that is owned by Pinpoint
// the client will retry requests by default with a expotential backoff
func NewHTTPAPIClientDefault() (httpclient.Client, error) {
	hcConfig := &httpclient.Config{
		Paginator: httpclient.NoPaginator(),
		Retryable: httpclient.NewBackoffRetry(10*time.Millisecond, 100*time.Millisecond, 10*time.Second, 2.0),
	}
	return NewHTTPAPIClient(hcConfig)
}

// absurl is absolute if matching our API url pattern
var absurl = regexp.MustCompile("^https://(.*)" + baseURL + "$")

func isAbsURL(urlstr string) bool {
	return absurl.MatchString(urlstr)
}

// Get will invoke api for channel and basepath
func Get(channel string, basepath string, apiKey string) (*http.Response, error) {
	bu := BackendURL(channel)
	var urlstr string
	if isAbsURL(basepath) {
		urlstr = basepath
	} else {
		u, _ := url.Parse(bu)
		u.Path = basepath
		urlstr = u.String()
	}
	req, _ := http.NewRequest(http.MethodGet, urlstr, nil)
	if apiKey != "" {
		req.Header.Add("x-api-key", apiKey)
	}
	req.Header.Set("Accept", "application/json")
	client, err := NewHTTPAPIClientDefault()
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Post will invoke api for channel and basepath as JSON post
func Post(channel string, basepath string, apiKey string, obj interface{}) (*http.Response, error) {
	bu := BackendURL(channel)
	var urlstr string
	if isAbsURL(basepath) {
		urlstr = basepath
	} else {
		u, _ := url.Parse(bu)
		u.Path = basepath
		urlstr = u.String()
	}
	req, _ := http.NewRequest(http.MethodPost, urlstr, strings.NewReader(json.Stringify(obj)))
	if apiKey != "" {
		req.Header.Add("x-api-key", apiKey)
	}
	req.Header.Set("Content-Type", "application/json")
	client, err := NewHTTPAPIClientDefault()
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
