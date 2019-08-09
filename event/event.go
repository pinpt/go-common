package event

import (
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

	"github.com/gorilla/websocket"
	"github.com/pinpt/go-common/api"
	"github.com/pinpt/go-common/datamodel"
	"github.com/pinpt/go-common/datetime"
	"github.com/pinpt/go-common/hash"
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
	ID        string                  `json:"message_id"`
	Timestamp time.Time               `json:"timestamp"`
	Type      string                  `json:"type"`
	Model     datamodel.ModelNameType `json:"model"`
	Headers   map[string]string       `json:"headers,omitempty"`
	Data      string                  `json:"data"`
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
			Success bool
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
	Type      string            `json:"type"`
	Model     string            `json:"model"`
	Data      string            `json:"object"`

	commitch chan bool
}

// Commit for committing a message when auto commit is false
func (e SubscriptionEvent) Commit() {
	e.commitch <- true
}

type action struct {
	ID     string      `json:"id"`
	Action string      `json:"action"`
	Data   interface{} `json:"data"`
}

type actionResponse struct {
	ID      string             `json:"id"`
	Success bool               `json:"success"`
	Message *string            `json:"message"`
	Data    *SubscriptionEvent `json:"data"`
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
	conn         *websocket.Conn
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
		if c.conn != nil {
			c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, ""))
			c.conn.Close()
		}
		close(c.done)
		close(c.ch)
	}
	return nil
}

var MaxErrorCount = 25

func (c *SubscriptionChannel) run() {
	origin := api.BackendURL(api.EventService, c.subscription.Channel)
	if strings.HasSuffix(origin, "/") {
		origin = origin[0 : len(origin)-1]
	}
	u := strings.ReplaceAll(pstrings.JoinURL(origin, "ws"), "https://", "wss://")
	headers := make(http.Header)
	headers.Set("Origin", origin)
	headers.Set(api.AuthorizationHeader, c.subscription.APIKey)

	if strings.Contains(u, "ppoint.io") {
		headers.Set("pinpt-customer-id", "5500a5ba8135f296")            // test case, doesn't work for real except local
		headers.Set("x-api-key", "fa0s8f09a8sd09f8iasdlkfjalsfm,.m,xf") // test case, doesn't work for real except local
	}

	var errors int

	for {
		c.mu.Lock()
		finished := c.closed
		c.mu.Unlock()
		if finished {
			break
		}
		wch, _, err := websocket.DefaultDialer.Dial(u, headers)
		if err != nil {
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
				break
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
				break
			}
		}

		// assign
		c.mu.Lock()
		c.conn = wch
		c.mu.Unlock()

		subaction := action{
			ID:     hash.Values(datetime.EpochNow(), c.subscription.APIKey, c.subscription.GroupID, c.subscription.Topics),
			Data:   pjson.Stringify(c.subscription),
			Action: "subscribe",
		}

		// send the subscription first
		if err := wch.WriteJSON(subaction); err != nil {
			if c.subscription.Errors != nil {
				c.subscription.Errors <- err
			} else {
				panic(err)
			}
			wch.Close()
			break
		}

		errors = 0
		var errored bool
		var closed bool
		var acked bool

		// now we just read messages until we're EOF
		for !closed {
			var actionresp actionResponse
			if err := wch.ReadJSON(&actionresp); err != nil {
				if err == io.EOF || websocket.IsCloseError(err) || websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
					closed = true
					errored = true
					break
				}
				c.mu.Lock()
				done := c.closed
				c.mu.Unlock()
				if done {
					closed = true
					break
				}
				if c.subscription.Errors != nil {
					c.subscription.Errors <- err
				} else {
					panic(err)
				}
				wch.Close()
				break
			}
			log.Debug(c.subscription.Logger, "received event", "response", actionresp)
			if subaction.ID == actionresp.ID {
				if !acked {
					// if the subscribe ack worked, great ... continue
					if actionresp.Success {
						acked = true
						continue
					}
					if c.subscription.Errors != nil {
						c.subscription.Errors <- fmt.Errorf(*actionresp.Message)
					} else {
						panic(*actionresp.Message)
					}
					wch.Close()
					break
				}
				if actionresp.Data != nil {
					c.mu.Lock()
					if !c.closed {
						subdata := actionresp.Data
						if c.subscription.DisableAutoCommit {
							subdata.commitch = make(chan bool)
						}
						c.ch <- *subdata
						if c.subscription.DisableAutoCommit {
							// wait for our commit before continuing
							select {
							case <-subdata.commitch:
								if err := wch.WriteJSON(action{actionresp.ID, "commit", subdata.ID}); err != nil {
									if c.subscription.Errors != nil {
										c.subscription.Errors <- err
									} else {
										panic(err)
									}
								}
								break
							case <-c.ctx.Done():
								break
							}
						}
					}
					c.mu.Unlock()
				}
				// check to see if we're done
				select {
				case <-c.ctx.Done():
					closed = true
					break
				case <-c.done:
					closed = true
					break
				default:
				}
			}
		}

		wch.Close()

		if errored {
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
		}
	}
}

// Subscription is the information for creating a subscription channel to receive events from the event server
type Subscription struct {
	GroupID           string            `json:"group_id"`
	Topics            []string          `json:"topics"`
	Headers           map[string]string `json:"headers"`
	IdleDuration      string            `json:"idle_duration"`
	Limit             int               `json:"limit"`
	Offset            string            `json:"offset"`
	After             int64             `json:"after"`
	DisableAutoCommit bool              `json:"disable_autocommit"`
	Channel           string            `json:"-"`
	APIKey            string            `json:"-"`
	BufferSize        int               `json:"-"`
	Errors            chan<- error      `json:"-"`
	Logger            log.Logger        `json:"-"`
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
