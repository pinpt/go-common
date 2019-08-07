package event

import (
	"bufio"
	"bytes"
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
	"github.com/pinpt/go-common/log"
	pstrings "github.com/pinpt/go-common/strings"
)

const jsonContentType = "application/json"

// PublishEvent is the container for a model event
type PublishEvent struct {
	Object  datamodel.Model
	Headers map[string]string
	Logger  log.Logger `json:"-"`
}

// EventPayload is the container for a model event
type payload struct {
	Type    string                  `json:"type"`
	Model   datamodel.ModelNameType `json:"model"`
	Headers map[string]string       `json:"headers,omitempty"`
	Data    string                  `json:"data"`
}

// Publish will publish an event to the event api server
func Publish(ctx context.Context, event PublishEvent, channel string, apiKey string, debug ...bool) (err error) {
	url := pstrings.JoinURL(api.BackendURL(api.EventService, channel), "ingest")
	payload := payload{
		Type:    "json",
		Model:   event.Object.GetModelName(),
		Headers: event.Headers,
		Data:    base64.StdEncoding.EncodeToString([]byte(event.Object.Stringify())),
	}
	if len(debug) > 0 && debug[0] {
		fmt.Println(pjson.Stringify(payload))
		fmt.Println(url)
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
	if event.Logger != nil {
		log.Debug(event.Logger, "sent event", "payload", payload, "event", event)
	}
	defer resp.Body.Close()
	bts, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	respStr := string(bts)
	if len(debug) > 0 && debug[0] {
		for k, v := range resp.Header {
			fmt.Println(k, "=>", v[0])
		}
		fmt.Println(respStr)
	}
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

var MaxErrorCount = 10

func (c *SubscriptionChannel) run() {
	u := pstrings.JoinURL(api.BackendURL(api.EventService, c.subscription.Channel), "consume")
	var errors int
	for {
		// check to see if we're done
		select {
		case <-c.ctx.Done():
			return
		case <-c.done:
			return
		default:
		}
		req, err := http.NewRequest(http.MethodPost, u, strings.NewReader(pjson.Stringify(c.subscription)))
		if err != nil {
			if c.subscription.Errors != nil {
				c.subscription.Errors <- err
			} else {
				panic(err)
			}
			return
		}
		req = req.WithContext(c.ctx)
		api.SetUserAgent(req)
		api.SetAuthorization(req, c.subscription.APIKey)
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("Accept", "application/x-ndjson")
		req.Header.Set("Connection", "keep-alive")
		var resp *http.Response
		if strings.Contains(u, "ppoint.io") {
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
		} else {
			resp, err = http.DefaultClient.Do(req) // use the default client so we can reasonable defaults and no retry
			if err != nil {
				// if shutdown/closed, go ahead and return
				c.mu.Lock()
				select {
				case <-c.ctx.Done():
					c.mu.Unlock()
					return
				case <-c.done:
					c.mu.Unlock()
					return
				default:
				}
				c.mu.Unlock()
				if c.subscription.Errors != nil {
					c.subscription.Errors <- err
				} else {
					panic(err)
				}
				return
			}
		}
		var isRetryableError bool
		if err != nil && (strings.Contains(err.Error(), "connect: connection refused") || err == io.EOF || err == io.ErrUnexpectedEOF || strings.Contains(err.Error(), "EOF")) {
			isRetryableError = true
		} else if err != nil {
			e := fmt.Errorf("error creating subscription. %v", err)
			if c.subscription.Errors != nil {
				c.subscription.Errors <- e
			} else {
				panic(e)
			}
			return
		}
		if resp != nil {
			log.Debug(c.subscription.Logger, "created a subscription", "status", resp.StatusCode, "subscription", c.subscription)
			if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusBadGateway {
				isRetryableError = true
			}
		}
		if isRetryableError {
			errors++
			// fmt.Println("got an error, will retry", resp.StatusCode, errors)
			if errors <= MaxErrorCount {
				// expotential backoff
				time.Sleep((time.Millisecond * 250) * time.Duration(errors))
				continue
			}
			e := fmt.Errorf("error creating subscription. the server appears to be down after %v attempts", errors)
			if c.subscription.Errors != nil {
				c.subscription.Errors <- e
			} else {
				panic(e)
			}
			return
		}
		// check the status code and if not OK, return the error
		if resp.StatusCode != http.StatusOK {
			if c.subscription.Errors != nil {
				var rerr struct {
					Error string `json:"message"`
				}
				buf, _ := ioutil.ReadAll(resp.Body)
				if err := json.Unmarshal(buf, &rerr); err != nil {
					c.subscription.Errors <- fmt.Errorf("error creating subscription: %v (status code=%d)", rerr.Error, resp.StatusCode)
				} else {
					c.subscription.Errors <- fmt.Errorf("error creating subscription: %v (status code=%d)", string(buf), resp.StatusCode)
				}
			} else {
				buf, _ := ioutil.ReadAll(resp.Body)
				panic("error receiving response: " + string(buf))
			}
			resp.Body.Close()
			return
		}
		errors = 0
		var wg sync.WaitGroup
		wg.Add(1)
		// start a go routine to read the response since it will block for a period of idle time reading one
		// event at a time as we receive it
		go func() {
			defer func() {
				resp.Body.Close()
				wg.Done()
			}()
			scanner := bufio.NewScanner(resp.Body)
			scanner.Split(bufio.ScanLines)
			for scanner.Scan() {
				if len(bytes.TrimSpace(scanner.Bytes())) > 0 {
					var payload SubscriptionEvent
					if err := json.Unmarshal(scanner.Bytes(), &payload); err != nil {
						if c.subscription.Errors != nil {
							c.subscription.Errors <- fmt.Errorf("error decoding subscription payload data: %v", err)
						} else {
							panic(err)
						}
					}
					log.Debug(c.subscription.Logger, "received event", "event", payload)
					c.mu.Lock()
					c.ch <- payload
					c.mu.Unlock()
				}
			}
			if scanner.Err() != nil {
				if scanner.Err() != context.Canceled && scanner.Err() != io.ErrUnexpectedEOF {
					if c.subscription.Errors != nil {
						c.subscription.Errors <- fmt.Errorf("error receiving subscription data: %v", scanner.Err())
					} else {
						panic(err)
					}
				}
			}
		}()
		wg.Wait()
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
	Logger       log.Logger        `json:"-"`
}

// NewSubscription will create a subscription to the event server and will continously read events (as they arrive)
// and send them back to the return channel. once you're done, you must call Close on the channel to stop
// receiving events
func NewSubscription(ctx context.Context, subscription Subscription) (*SubscriptionChannel, error) {
	if subscription.Logger == nil {
		subscription.Logger = log.NewNoOpTestLogger()
	}
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
