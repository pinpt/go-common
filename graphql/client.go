package graphql

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"net/http"
	purl "net/url"
	"time"

	pauth "github.com/pinpt/go-common/auth"
	"github.com/pinpt/go-common/hash"
	pjson "github.com/pinpt/go-common/json"
)

// Client interface to client object
type Client interface {
	// Mutate makes _mutate_ http request to the GraphQL server
	Mutate(query string, variables Variables, out interface{}) error
	// Mutate makes _query_ http request to the GraphQL server
	Query(query string, variables Variables, out interface{}) error
}

// Variables a simple alias to map[string]interface{}
type Variables map[string]interface{}

// NewClient returns a new instance of Client
func NewClient(customerID string, userID string, apikey string, url string) (Client, error) {
	headers, err := defaultHeaders(customerID, userID, apikey)
	if err != nil {
		return nil, err
	}
	u, err := purl.Parse(url)
	if err != nil {
		return nil, err
	}
	u.Path = "/graphql"
	return &client{
		headers: headers,
		url:     u.String(),
	}, nil
}

func defaultHeaders(customerID string, userID string, apikey string) (map[string]string, error) {
	headers := map[string]string{}
	headers["x-api-key"] = apikey
	if customerID != "" {
		var header struct {
			CustomerID string `json:"customer_id,omitempty"`
			UserID     string `json:"user_id,omitempty"`
		}
		header.CustomerID = customerID
		header.UserID = userID
		auth, err := pauth.EncryptString(pjson.Stringify(header), apikey)
		if err != nil {
			return nil, err
		}
		headers["x-api-customer"] = auth
	}
	return headers, nil
}

type client struct {
	headers map[string]string
	url     string
}

// Mutate makes _mutate_ http request to the GraphQL server
func (c *client) Mutate(query string, variables Variables, out interface{}) error {
	return c.do(query, variables, out)
}

// Mutate makes _query_ http request to the GraphQL server
func (c *client) Query(query string, variables Variables, out interface{}) error {
	return c.do(query, variables, out)
}

func (c *client) do(query string, variables Variables, out interface{}) error {

	payload := struct {
		Variables Variables `json:"variables"`
		Query     string    `json:"query"`
	}{variables, query}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, c.url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Add("x-trace-id", hash.Values(9999*rand.Float64(), time.Now().Unix()))
	req.Header.Add("content-type", "application/json")
	for k, v := range c.headers {
		req.Header.Add(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var datares struct {
		Data   interface{} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	err = json.Unmarshal(body, &datares)
	if err != nil {
		return err
	}
	if datares.Errors != nil {
		b, err := json.Marshal(datares.Errors)
		if err != nil {
			return err
		}
		return errors.New(string(b))
	}
	b, err := json.Marshal(datares.Data)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, out)
	return err
}
