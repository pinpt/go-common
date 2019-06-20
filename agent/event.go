package events

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/pinpt/go-common/api"
	pjson "github.com/pinpt/go-common/json"
	pstrings "github.com/pinpt/go-common/strings"
)

type EventAPI struct {
	Type    string            `json:"type"`
	Model   string            `json:"model"`
	Headers map[string]string `json:"headers"`
	Data    string            `json:"data"`
}

func (e *EventAPI) Reader() (io.Reader, error) {

	bts, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(bts), nil
}

type Event struct {
	// built in types

	ID         string `json:"id" bson:"_id" yaml:"id" faker:"-"`
	RefID      string `json:"ref_id" bson:"ref_id" yaml:"ref_id" faker:"-"`
	RefType    string `json:"ref_type" bson:"ref_type" yaml:"ref_type" faker:"-"`
	CustomerID string `json:"customer_id" bson:"customer_id" yaml:"customer_id" faker:"-"`
	Hashcode   string `json:"hashcode" bson:"hashcode" yaml:"hashcode" faker:"-"`
	// custom types

	// Type type
	Type string `json:"type" bson:"type" yaml:"type" faker:"-"`
	// UUID uuid
	UUID string `json:"uuid" bson:"uuid" yaml:"uuid" faker:"-"`
	// OS os
	OS string `json:"os" bson:"os" yaml:"os" faker:"-"`
	// Distro distro
	Distro string `json:"distro" bson:"distro" yaml:"distro" faker:"-"`
	// Version agent version
	Version string `json:"version" bson:"version" yaml:"version" faker:"-"`
	// Hostname hostname
	Hostname string `json:"hostname" bson:"hostname" yaml:"hostname" faker:"-"`
	// NumCPU num cpus
	NumCPU int64 `json:"num_cpu" bson:"num_cpu" yaml:"num_cpu" faker:"-"`
	// FreeSpace free space
	FreeSpace int64 `json:"free_space" bson:"free_space" yaml:"free_space" faker:"-"`
	// GoVersion go version
	GoVersion string `json:"go_version" bson:"go_version" yaml:"go_version" faker:"-"`
	// Architecture architecture
	Architecture string `json:"architecture" bson:"architecture" yaml:"architecture" faker:"-"`
	// Memory memory
	Memory int64 `json:"memory" bson:"memory" yaml:"memory" faker:"-"`
	// Date date
	Date string `json:"date" bson:"date" yaml:"date" faker:"-"`
	// Error error
	Error string `json:"error" bson:"error" yaml:"error" faker:"-"`
	// Message message
	Message string `json:"message" bson:"message" yaml:"message" faker:"-"`
}

func (e *Event) Base64String() (string, error) {

	bts, err := json.Marshal(e)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(bts), nil
}

func PostEvent(ctx context.Context, event Event, channel string, apiKey string, headers map[string]string) error {
	URL := api.BackendURL(api.EventService, channel)

	base64String, err := event.Base64String()
	if err != nil {
		return err
	}

	eventAPI := EventAPI{
		Type:    "json",
		Model:   "agent.Event",
		Headers: headers,
		Data:    base64String,
	}

	URL = pstrings.JoinURL(URL, "ingest")

	csrfToken, err := getCSRFToken(ctx, channel, apiKey)
	if err != nil {
		return err
	}

	req, _ := http.NewRequest(http.MethodPost, URL, strings.NewReader(pjson.Stringify(eventAPI)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-CSRF-Token", csrfToken)
	api.SetUserAgent(req)
	api.SetAuthorization(req, apiKey)
	req = req.WithContext(ctx)
	var resp *http.Response
	if strings.Contains(URL, "ppoint.io") {
		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
		resp, err = client.Do(req)
	} else {
		client, err := api.NewHTTPAPIClientDefault()
		if err != nil {
			return err
		}
		resp, err = client.Do(req)
	}
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	bts, err := ioutil.ReadAll(resp.Body)

	respStr := string(bts)

	if respStr != "OK" {
		return fmt.Errorf("%s", respStr)
	}

	return nil
}

func getCSRFToken(ctx context.Context, channel string, apiKey string) (string, error) {

	URL := api.BackendURL(api.EventService, channel)
	URL = pstrings.JoinURL(URL, "token")
	req, err := http.NewRequest(http.MethodGet, URL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	api.SetAuthorization(req, apiKey)
	api.SetUserAgent(req)
	req = req.WithContext(ctx)
	var resp *http.Response
	if strings.Contains(URL, "ppoint.io") {
		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
		resp, err = client.Do(req)
	} else {
		client, err := api.NewHTTPAPIClientDefault()
		if err != nil {
			return "", err
		}
		resp, err = client.Do(req)
	}
	if err != nil {
		return "", err
	}

	csrfToken, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	resp.Body.Close()

	return string(csrfToken), nil
}
