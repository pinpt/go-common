package kafka

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/linkedin/goavro"
	pjson "github.com/pinpt/go-common/json"
)

var (
	ErrSchemaNotFound  = errors.New("Schema not found")
	ErrSubjectNotFound = errors.New("Subject not found")
)

const (
	subjectNotFoundCode = 40401
	schemaNotFoundCode  = 40403
	contentType         = "application/vnd.schemaregistry.v1+json"
)

// RegistryClient is an interface to the registry
type RegistryClient interface {
	CreateSubject(string, *goavro.Codec) (int, error)
	IsSchemaRegistered(string, *goavro.Codec) bool
	Ping() bool
}

// RegistryConfig is the config for connecting to the registry server
type RegistryConfig struct {
	URL      string
	Username string
	Password string
}

// HTTPRegistryClient is an implementation of the RegistryClient interface
type HTTPRegistryClient struct {
	config RegistryConfig
	cache  map[string]int
	mu     sync.RWMutex
}

var _ RegistryClient = (*HTTPRegistryClient)(nil)

type createSubjResp struct {
	ID        int    `json:"id"`
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

func (c *HTTPRegistryClient) setHeaders(req *http.Request) {
	req.Header.Set("Content-Type", contentType)
	if c.config.Username != "" {
		req.SetBasicAuth(c.config.Username, c.config.Password)
	}
}

type schemaReq struct {
	Schema string `json:"schema"`
}

func (c *HTTPRegistryClient) CreateSubject(subject string, codec *goavro.Codec) (int, error) {
	c.mu.RLock()
	val := c.cache[subject]
	c.mu.RUnlock()
	if val > 0 {
		return val, nil
	}
	u, err := url.Parse(c.config.URL)
	if err != nil {
		return 0, err
	}
	u.Path = "/subjects/" + subject + "/versions"
	req, err := http.NewRequest(http.MethodPost, u.String(), strings.NewReader(pjson.Stringify(schemaReq{codec.Schema()})))
	if err != nil {
		return 0, err
	}
	c.setHeaders(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var r createSubjResp
	if err := json.Unmarshal(buf, &r); err != nil {
		return 0, err
	}
	if r.ErrorCode > 0 {
		switch r.ErrorCode {
		case subjectNotFoundCode:
			return 0, ErrSubjectNotFound
		case schemaNotFoundCode:
			return 0, ErrSchemaNotFound
		default:
			return 0, errors.New(r.Message)
		}
	}
	c.mu.Lock()
	c.cache[subject] = r.ID
	c.mu.Unlock()
	return r.ID, nil
}

func (c *HTTPRegistryClient) IsSchemaRegistered(subject string, codec *goavro.Codec) bool {
	res, err := c.CreateSubject(subject, codec)
	if res > 0 && err == nil {
		return true
	}
	return false
}

func (c *HTTPRegistryClient) Ping() bool {
	u, err := url.Parse(c.config.URL)
	if err != nil {
		return false
	}
	u.Path = "/config"
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return false
	}
	c.setHeaders(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// NewRegistryClient will create a registry client
func NewRegistryClient(config RegistryConfig) *HTTPRegistryClient {
	return &HTTPRegistryClient{
		config: config,
		cache:  make(map[string]int),
	}
}
