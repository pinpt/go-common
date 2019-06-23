package kafka

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/linkedin/goavro"
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
	ID        int `json:"id"`
	ErrorCode int `json:"error_code"`
}

func (c *HTTPRegistryClient) setHeaders(req *http.Request) {
	req.Header.Set("Content-Type", contentType)
	if c.config.Username != "" {
		req.SetBasicAuth(c.config.Username, c.config.Password)
	}
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
	u.Path = "/subjects/" + subject
	req, err := http.NewRequest(http.MethodPost, u.String(), strings.NewReader(codec.Schema()))
	if err != nil {
		return 0, err
	}
	c.setHeaders(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var r createSubjResp
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return 0, err
	}
	switch r.ErrorCode {
	case subjectNotFoundCode:
		return 0, ErrSubjectNotFound
	case schemaNotFoundCode:
		return 0, ErrSchemaNotFound
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

// NewRegistryClient will create a registry client
func NewRegistryClient(config RegistryConfig) *HTTPRegistryClient {
	return &HTTPRegistryClient{
		config: config,
		cache:  make(map[string]int),
	}
}
