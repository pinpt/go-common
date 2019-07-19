package event

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pinpt/go-common/api"
	"github.com/pinpt/go-common/datamodel"
	pjson "github.com/pinpt/go-common/json"
	pstrings "github.com/pinpt/go-common/strings"
)

const jsonContentType = "application/json"

// PublishEvent is the container for a model event
type PublishEvent struct {
	Object  datamodel.Model
	Headers map[string]string
}

// EventPayload is the container for a model event
type payload struct {
	Type    string                  `json:"type"`
	Model   datamodel.ModelNameType `json:"model"`
	Headers map[string]string       `json:"headers,omitempty"`
	Data    string                  `json:"data"`
}

// Publish will publish an event to the event api server
func Publish(ctx context.Context, event PublishEvent, channel string, apiKey string) (err error) {
	url := pstrings.JoinURL(api.BackendURL(api.EventService, channel), "ingest")
	payload := payload{
		Type:    "json",
		Model:   event.Object.GetModelName(),
		Headers: event.Headers,
		Data:    base64.StdEncoding.EncodeToString([]byte(event.Object.Stringify())),
	}
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(pjson.Stringify(payload)))
	req.Header.Set("Content-Type", jsonContentType)
	req.Header.Set("Accept", jsonContentType)
	api.SetUserAgent(req)
	api.SetAuthorization(req, apiKey)
	req = req.WithContext(ctx)
	var resp *http.Response
	if strings.Contains(url, "ppoint.io") {
		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
		resp, err = client.Do(req)
		if err != nil {
			return err
		}
	} else {
		client, err := api.NewHTTPAPIClientDefault()
		if err != nil {
			return err
		}
		resp, err = client.Do(req)
		if err != nil {
			return err
		}
	}
	defer resp.Body.Close()
	bts, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	respStr := string(bts)
	if respStr != "OK" {
		var rerr struct {
			Message string
		}
		if err := json.Unmarshal(bts, &rerr); err != nil {
			return fmt.Errorf("%s", respStr)
		}
		return errors.New(rerr.Message)
	}
	return nil
}

// SubscriptionEvent is received from the event server
type SubscriptionEvent struct {
	Timestamp time.Time         `json:"timestamp"`
	Headers   map[string]string `json:"headers,omitempty"`
	Key       string            `json:"key"`
	Type      string            `json:"type"`
	Model     string            `json:"model"`
	Data      string            `json:"object"`
	Offset    string            `json:"offset,omitempty"`
}

// SubscriptionChannel is a channel for receiving events
type SubscriptionChannel struct {
	ctx          context.Context
	ch           chan SubscriptionEvent
	done         chan bool
	subscription Subscription
	mu           sync.Mutex
	closed       bool
	cancel       context.CancelFunc
}

// Channel returns a read-only channel to receive SubscriptionEvent
func (c *SubscriptionChannel) Channel() <-chan SubscriptionEvent {
	return c.ch
}

// Close will close the event channel and stop receiving them
func (c *SubscriptionChannel) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.cancel()
		c.closed = true
		c.done <- true
		close(c.done)
		close(c.ch)
	}
	return nil
}

func (c *SubscriptionChannel) run() {
	url := pstrings.JoinURL(api.BackendURL(api.EventService, c.subscription.Channel), "consume")
	for {
		// check to see if we're done
		select {
		case <-c.ctx.Done():
			return
		case <-c.done:
			return
		default:
		}
		req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(pjson.Stringify(c.subscription)))
		if err != nil {
			if c.subscription.Errors != nil {
				c.subscription.Errors <- err
			}
			return
		}
		req = req.WithContext(c.ctx)
		req.Header.Set("Content-Type", jsonContentType)
		req.Header.Set("Accept", jsonContentType)
		api.SetUserAgent(req)
		api.SetAuthorization(req, c.subscription.APIKey)
		var resp *http.Response
		if strings.Contains(url, "ppoint.io") {
			client := &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: true,
					},
				},
			}
			req.Header.Set("pinpt-customer-id", "5500a5ba8135f296")            // test case, doesn't work for real except local
			req.Header.Set("x-api-key", "fa0s8f09a8sd09f8iasdlkfjalsfm,.m,xf") // test case, doesn't work for real except local
			resp, err = client.Do(req)
			if err != nil {
				if c.subscription.Errors != nil {
					c.subscription.Errors <- err
				}
				return
			}
		} else {
			client, err := api.NewHTTPAPIClientDefault()
			if err != nil {
				if c.subscription.Errors != nil {
					c.subscription.Errors <- err
				}
				return
			}
			resp, err = client.Do(req)
			if err != nil {
				if c.subscription.Errors != nil {
					c.subscription.Errors <- err
				}
				return
			}
		}
		// check the status code and if not OK, return the error
		if resp.StatusCode != http.StatusOK {
			if c.subscription.Errors != nil {
				var rerr struct {
					Error string `json:"message"`
				}
				json.NewDecoder(resp.Body).Decode(&rerr)
				c.subscription.Errors <- fmt.Errorf("error creating subscription: %v", rerr.Error)
			}
			resp.Body.Close()
			return
		}
		finished := make(chan bool)
		// start a go routine to read the response since it will block for a period of idle time reading one
		// event at a time as we receive it
		go func() {
			defer func() {
				resp.Body.Close()
				finished <- true
				close(finished)
			}()
			r := bufio.NewReader(resp.Body)
			for {
				// read one line at a time as we receive it
				buf, err := r.ReadBytes('\n')
				if err == context.Canceled || err == io.EOF {
					return
				}
				if buf != nil && len(buf) > 0 {
					var payload SubscriptionEvent
					if err := json.Unmarshal(buf, &payload); err != nil {
						if c.subscription.Errors != nil {
							c.subscription.Errors <- fmt.Errorf("error decoding subscription payload data: %v", err)
						}
						return
					}
					// check once more that we're not cancelled
					c.mu.Lock()
					select {
					case <-c.done:
						c.mu.Unlock()
						return
					default:
						c.ch <- payload
						c.mu.Unlock()
					}
				} else {
					select {
					case <-c.done:
						return
					default:
					}
				}
			}
		}()
		// block until either we're closed or we finish the stream
		// and then loop and do it again
		select {
		case <-c.done:
			return
		case <-finished:
			continue
		}
	}
}

// Subscription is the information for creating a subscription channel to receive events from the event server
type Subscription struct {
	GroupID      string            `json:"group_id"`
	Topics       []string          `json:"topics"`
	Headers      map[string]string `json:"headers"`
	IdleDuration string            `json:"idle_duration"`
	Limit        int               `json:"limit"`
	Offset       string            `json:"offset"`
	Channel      string            `json:"-"`
	APIKey       string            `json:"-"`
	BufferSize   int               `json:"-"`
	Errors       chan<- error      `json:"-"`
}

// NewSubscription will create a subscription to the event server and will continously read events (as they arrive)
// and send them back to the return channel. once you're done, you must call Close on the channel to stop
// receiving events
func NewSubscription(ctx context.Context, subscription Subscription) (*SubscriptionChannel, error) {
	newctx, cancel := context.WithCancel(ctx)
	subch := &SubscriptionChannel{
		ctx:          newctx,
		cancel:       cancel,
		ch:           make(chan SubscriptionEvent, subscription.BufferSize),
		done:         make(chan bool, 1),
		subscription: subscription,
	}
	go subch.run()
	return subch, nil
}
