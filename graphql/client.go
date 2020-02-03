package graphql

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	purl "net/url"

	"github.com/pinpt/go-common/api"

	pauth "github.com/pinpt/go-common/auth"
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

// New for use outside of the pinpoint org
func New(url string, apikey string, headers map[string]string) Client {
	return &client{
		headers: headers,
		url:     url,
		apikey:  apikey,
	}
}

func defaultHeaders(customerID string, userID string, apikey string) (map[string]string, error) {
	headers := map[string]string{}
	if apikey != "" {
		headers["x-api-key"] = apikey
	}
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
	headers["content-type"] = "application/json"
	return headers, nil
}

type client struct {
	headers map[string]string
	url     string
	apikey  string
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
	if c.apikey != "" {
		api.SetAuthorization(req, c.apikey)
	}
	for k, v := range c.headers {
		req.Header.Add(k, v)
	}

	httpclient, err := api.NewHTTPAPIClientDefault()
	if err != nil {
		return err
	}

	resp, err := httpclient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
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
		return json.Unmarshal(b, out)
	}
	return fmt.Errorf("err: %s. status code: %s", string(body), resp.Status)
}
