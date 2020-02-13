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
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/pinpt/go-common/httpdefaults"
	"github.com/pinpt/go-common/json"
	"github.com/pinpt/httpclient"
)

// baseURL is the url to the Pinpoint Cloud API
const baseURL = "pinpoint.com/"
const devbaseURL = "ppoint.io"

// AuthorizationHeader is the name of the authorization header to use
const AuthorizationHeader = "Authorization"

const userAgentFormat = "Pinpoint Agent/%s"

var (
	// domainRegex are the only domains that we trust
	domainRegex = regexp.MustCompile("\\.(pinpt|ppoint|pinpoint)\\.(net|com|io|co)$")

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
		"60b87575447dcba2a36b7d11ac09fb24a9db406fee12d2cc90180517616e8a18": true, //CN=Let's Encrypt Authority X3,O=Let's Encrypt,C=US
	}
	caWeTrustDev = map[string]bool{
		"e1ae9c3de848ece1ba72e0d991ae4d0d9ec547c6bad1dddab9d6beb0a7e0e0d8": true, //C=GB/ST=Greater Manchester/L=Salford/O=Sectigo Limited/CN=Sectigo RSA Domain Validation Secure Server CA
	}
)

// isTrusted returns true if the Certificate is a CA Certificate that we explicitly trust
func isTrusted(cert *x509.Certificate, addr string) bool {
	der, _ := x509.MarshalPKIXPublicKey(cert.PublicKey)
	hash := sha256.Sum256(der)
	fingerprint := fmt.Sprintf("%x", hash)
	// check valid production CA
	if caWeTrust[fingerprint] {
		return true
	}
	// check validate local dev
	if caWeTrustDev[fingerprint] && strings.Contains(addr, devbaseURL) {
		return true
	}
	return false
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

const (
	// AgentService is the agent service endpoint
	AgentService = "agent.api"
	// AuthService is the auth service endpoint
	AuthService = "auth.api"
	// EventService is the event service endpoint
	EventService = "event.api"
)

// BackendURL return the base url to the API server
func BackendURL(subdomain string, channel string) string {
	switch channel {
	// internal services set through environment variables
	case "internal":
		switch subdomain {
		case AgentService:
			return os.Getenv("PP_AGENT_SERVICE")
		case AuthService:
			return os.Getenv("PP_AUTH_SERVICE")
		case EventService:
			return os.Getenv("PP_EVENT_SERVICE")
		}
	case "stable", "":
		return fmt.Sprintf("https://%s.%s", subdomain, baseURL)
	case "dev":
		switch subdomain {
		case AgentService:
			return fmt.Sprintf("https://%s.%s:3004/", subdomain, devbaseURL)
		case AuthService:
			return fmt.Sprintf("https://%s.%s:3000/", subdomain, devbaseURL)
		case EventService:
			return fmt.Sprintf("https://%s.%s:8443/", subdomain, devbaseURL)
		}
	}
	return fmt.Sprintf("https://%s.%s.%s", subdomain, channel, baseURL)
}

func incasesensitiveEnv(name string) string {
	val := strings.ToLower(name)
	for _, env := range os.Environ() {
		kv := strings.Split(env, "=")
		if strings.ToLower(kv[0]) == val {
			return kv[1]
		}
	}
	return ""
}

func isUsingProxy() bool {
	return incasesensitiveEnv("https_proxy") != "" ||
		incasesensitiveEnv("http_proxy") != "" ||
		incasesensitiveEnv("all_proxy") != ""
}

// NewHTTPAPIClient will return a new HTTP client for talking with the Pinpoint API
// it will only allow a trusted TLS connection with a valid TLS certificate signed
// by a trusted Certificate Authority and for a DNS name that is owned by Pinpoint
func NewHTTPAPIClient(config *httpclient.Config) (httpclient.Client, error) {
	t := httpdefaults.DefaultTransport()
	// we can only reliably do TLS cert verfication if we're not using a proxy server
	if !isUsingProxy() {
		t.DialTLS = func(network, addr string) (net.Conn, error) {
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
				if cert.IsCA && isTrusted(cert, addr) {
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
	} else {
		// if we're using a proxy server, we also need to turn off support for HTTP/2
		t.TLSNextProto = make(map[string]func(authority string, c *tls.Conn) http.RoundTripper)
	}
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

// NewHTTPAPIClientDefaultWithTimeout will return a new HTTP client for talking with the Pinpoint API
// it will only allow a trusted TLS connection with a valid TLS certificate signed
// by a trusted Certificate Authority and for a DNS name that is owned by Pinpoint
// the client will retry requests by default with a expotential backoff. The max duration to wait
// is supplied for how long to take at most to try and send data
func NewHTTPAPIClientDefaultWithTimeout(duration time.Duration) (httpclient.Client, error) {
	hcConfig := &httpclient.Config{
		Paginator: httpclient.NoPaginator(),
		Retryable: httpclient.NewBackoffRetry(10*time.Millisecond, 100*time.Millisecond, duration, 2.0),
	}
	return NewHTTPAPIClient(hcConfig)
}

// absurl is absolute if matching our API url pattern
var absurl = regexp.MustCompile("^https?://(.*)" + baseURL + "$")

func isAbsURL(urlstr string) bool {
	return absurl.MatchString(urlstr)
}

// SetUserAgent will set the agent user agent
func SetUserAgent(req *http.Request) {
	req.Header.Set("User-Agent", fmt.Sprintf(userAgentFormat, os.Getenv("PP_AGENT_VERSION")))
}

// SetAuthorization will set the authorization header
func SetAuthorization(req *http.Request, apikey string) {
	if apikey != "" {
		req.Header.Set(AuthorizationHeader, apikey)
	}
}

// Get will invoke api for channel and basepath
func Get(ctx context.Context, channel string, service string, basepath string, apiKey string) (*http.Response, error) {
	bu := BackendURL(service, channel)
	var urlstr string
	if isAbsURL(basepath) {
		urlstr = basepath
	} else {
		u, _ := url.Parse(bu)
		u.Path = path.Join(u.Path, basepath)
		urlstr = u.String()
	}
	req, _ := http.NewRequest(http.MethodGet, urlstr, nil)
	req.Header.Set("Accept", "application/json")
	SetAuthorization(req, apiKey)
	SetUserAgent(req)
	req = req.WithContext(ctx)
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
func Post(ctx context.Context, channel string, service string, basepath string, apiKey string, obj interface{}) (*http.Response, error) {
	bu := BackendURL(service, channel)
	var urlstr string
	if isAbsURL(basepath) {
		urlstr = basepath
	} else {
		u, _ := url.Parse(bu)
		u.Path = path.Join(u.Path, basepath)
		urlstr = u.String()
	}
	req, _ := http.NewRequest(http.MethodPost, urlstr, strings.NewReader(json.Stringify(obj)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	SetUserAgent(req)
	SetAuthorization(req, apiKey)
	req = req.WithContext(ctx)
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
