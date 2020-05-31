package event

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pinpt/go-common/api"
	"github.com/pinpt/go-common/datamodel"
	pjson "github.com/pinpt/go-common/json"
	"github.com/pinpt/go-common/log"
	pstrings "github.com/pinpt/go-common/strings"
)

// EventDebug allows debug printing without requiring a logger to make it easy to turn on for debugging on the fly with an env variable
var EventDebug = os.Getenv("PP_EVENT_DEBUG") == "1"

const jsonContentType = "application/json"

// PublishEvent is the container for a model event
type PublishEvent struct {
	Object  datamodel.Model
	Headers map[string]string
	Logger  log.Logger `json:"-"`

	// for testing only
	url string
}

// payload is the container for a model event
type payload struct {
	ID        string                  `json:"message_id"`
	Timestamp time.Time               `json:"timestamp"`
	Type      string                  `json:"type"`
	Model     datamodel.ModelNameType `json:"model"`
	Headers   map[string]string       `json:"headers,omitempty"`
	Data      string                  `json:"data"`
}

// PublishConfig is used by Options
type PublishConfig struct {
	Debug     bool
	Deadline  time.Time
	Logger    log.Logger
	Header    http.Header
	Timestamp time.Time
	Async     bool // only when using SubscriptionChannel
}

// Option will allow publish to be customized
type Option func(config *PublishConfig) error

// WithDebugOption will turn on debugging
func WithDebugOption() Option {
	return func(config *PublishConfig) error {
		config.Debug = true
		return nil
	}
}

// WithDeadline will set a deadline for publishing
func WithDeadline(deadline time.Time) Option {
	return func(config *PublishConfig) error {
		config.Deadline = deadline
		return nil
	}
}

// WithLogger will provide a logger to use
func WithLogger(logger log.Logger) Option {
	return func(config *PublishConfig) error {
		config.Logger = logger
		return nil
	}
}

// WithTimestamp will set the timestamp on the event payload
func WithTimestamp(ts time.Time) Option {
	return func(config *PublishConfig) error {
		config.Timestamp = ts
		return nil
	}
}

// WithHeaders will provide an ability to set specific headers on the outgoing HTTP request
func WithHeaders(headers map[string]string) Option {
	return func(config *PublishConfig) error {
		for k, v := range headers {
			config.Header.Set(k, v)
		}
		return nil
	}
}

// WithAsync will set the async flag on publishing when using a SubscriptionChannel and is ignored
// when using the Publish method of the package. If true, is non-blocking and won't wait for an ACK.
// If false, will wait for the ack before returning from publish
func WithAsync(async bool) Option {
	return func(config *PublishConfig) error {
		config.Async = async
		return nil
	}
}

// ErrDeadlineExceeded is an error that's raised when a deadline occurs
var ErrDeadlineExceeded = errors.New("error: deadline exceeded on publish")

// IsHTTPStatusRetryable returns true if the status code is retryable
func IsHTTPStatusRetryable(statusCode int) bool {
	switch statusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusNotFound, http.StatusRequestTimeout, http.StatusTooManyRequests:
		return true
	}
	return false
}

// IsErrorRetryable returns true if the error is retryable
func IsErrorRetryable(err error) bool {
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return true
		}
		msg := err.Error()
		if strings.Contains(msg, "connect: connection refused") ||
			strings.Contains(msg, "EOF") ||
			strings.Contains(msg, "websocket: bad handshake") ||
			strings.Contains(msg, "i/o timeout") ||
			strings.Contains(msg, "connection reset by peer") ||
			strings.Contains(msg, "broken pipe") ||
			strings.Contains(msg, "websocket: close sent") {
			return true
		}
	}
	return false
}

// MinCompressionSize is the minimum size for compression on Publish
var MinCompressionSize = 1024

// cache the client so that we can reuse connections
var insecureClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	},
}

// cache the secure client so we can reuse connections
var secureClient, _ = api.NewHTTPAPIClientDefaultWithTimeout(time.Minute * 2)

// Publish will publish an event to the event api server
func Publish(ctx context.Context, event PublishEvent, channel string, apiKey string, options ...Option) (err error) {
	url := pstrings.JoinURL(api.BackendURL(api.EventService, channel), "ingest")
	if event.url != "" {
		url = event.url // for testing only
	}
	payload := payload{
		Type:    "json",
		Model:   event.Object.GetModelName(),
		Headers: event.Headers,
		Data:    base64.StdEncoding.EncodeToString([]byte(event.Object.Stringify())),
	}
	headers := make(http.Header)
	config := &PublishConfig{
		Debug:     false,
		Header:    headers,
		Deadline:  time.Now().Add(time.Minute * 30), // default is 30m to send event before we fail
		Timestamp: time.Now(),
	}
	var attempts int
	for {
		attempts++
		for _, opt := range options {
			if err := opt(config); err != nil {
				return err
			}
		}
		if !config.Deadline.IsZero() && config.Deadline.Before(time.Now()) {
			err = ErrDeadlineExceeded
			return
		}
		if config.Debug || EventDebug {
			fmt.Println("[event-api] sending payload", pjson.Stringify(payload), "to", url)
		}
		logger := config.Logger
		if logger == nil {
			logger = event.Logger
		}
		payload.Timestamp = config.Timestamp
		buf := pjson.Stringify(payload)
		var bufreader io.Reader
		var compressed bool
		if len(buf) > MinCompressionSize {
			var w bytes.Buffer
			cw := gzip.NewWriter(&w)
			cw.Write([]byte(buf))
			cw.Flush()
			cw.Close()
			compressed = true
			bufreader = &w
		} else {
			bufreader = strings.NewReader(buf)
		}
		req, _ := http.NewRequest(http.MethodPost, url, bufreader)
		req.Header.Set("Content-Type", jsonContentType)
		req.Header.Set("Accept", jsonContentType)
		if compressed {
			req.Header.Set("Encoding", "gzip")
		}
		api.SetUserAgent(req)
		api.SetAuthorization(req, apiKey)
		if len(headers) > 0 {
			for k, vals := range headers {
				req.Header.Set(k, vals[0])
			}
		}
		req = req.WithContext(ctx)
		var resp *http.Response
		var resperr error
		if strings.Contains(url, "ppoint.io") || strings.Contains(url, "localhost") || strings.Contains(url, "127.0.0.1:") || strings.Contains(url, "host.docker.internal") {
			resp, resperr = insecureClient.Do(req)
		} else {
			resp, resperr = secureClient.Do(req)
		}
		if resperr != nil {
			if IsErrorRetryable(resperr) {
				if logger != nil {
					log.Debug(logger, "error sending event, will retry", "event", event, "attempts", attempts)
				} else if EventDebug {
					fmt.Println("[event-api] error sending event", event, "error", resperr)
				}
				time.Sleep(time.Millisecond * time.Duration(100*attempts))
				continue
			}
			if logger != nil {
				log.Error(logger, "sent event error", "payload", payload, "event", event, "err", err)
			} else if EventDebug {
				fmt.Println("[event-api] sent event error", "payload", payload, "event", event, "err", err)
			}
			return resperr
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
			// if retryable, we'll continue again
			if IsHTTPStatusRetryable(resp.StatusCode) {
				resp.Body.Close()
				if logger != nil {
					log.Debug(logger, "publish encountered a retryable error, will retry again")
				} else if EventDebug {
					fmt.Println("[event-api] publish encountered a retryable error, will retry again", "code", resp.StatusCode)
				}
				time.Sleep(time.Millisecond * time.Duration(100*attempts))
				continue
			}
		} else {
			if logger != nil {
				log.Debug(logger, "sent event", "payload", payload, "event", event)
			} else if EventDebug {
				fmt.Println("[event-api] sent event", "payload", payload, "event", event)
			}
		}
		defer resp.Body.Close()
		bts, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		respStr := string(bts)
		if config.Debug || EventDebug {
			for k, v := range resp.Header {
				fmt.Println("[event-api] response header", k, "=>", v[0])
			}
			fmt.Println("[event-api] response body: " + respStr)
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
}

// SubscriptionEvent is received from the event server
type SubscriptionEvent struct {
	ID        string            `json:"message_id"`
	Timestamp time.Time         `json:"timestamp"`
	Headers   map[string]string `json:"headers,omitempty"`
	Key       string            `json:"key"`
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

type publishAck struct {
	ch    chan struct{}
	async bool
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
	ready        chan bool
	isready      bool
	headers      map[string]string

	lastPingTimer *time.Ticker
	lastPingsDone chan bool
	lastPing      time.Time
	lastPingMu    sync.Mutex

	writeMu        sync.RWMutex
	writeCallbacks map[string]*publishAck
}

// WaitForReady will block until we have received the subscription ack
func (c *SubscriptionChannel) WaitForReady() {
	<-c.ready
}

// Channel returns a read-only channel to receive SubscriptionEvent
func (c *SubscriptionChannel) Channel() <-chan SubscriptionEvent {
	return c.ch
}

// Close will close the event channel and stop receiving them
func (c *SubscriptionChannel) Close() error {
	ctx, cancel := context.WithCancel(c.ctx)
	go func() {
		defer cancel()
		dur := c.subscription.CloseTimeout
		if dur <= 0 {
			dur = time.Second * 10
		}
		select {
		case <-time.After(dur): // block up to 10s before forcing close
			c.cancel()
		case <-ctx.Done():
			return
		}
	}()
	var stopped bool
	// wait for all our pending write callbacks to finish receiving ACK
	for !stopped {
		c.writeMu.RLock()
		count := len(c.writeCallbacks)
		c.writeMu.RUnlock()
		if count == 0 {
			break
		}
		select {
		case <-ctx.Done():
			stopped = true
			break
		case <-time.After(time.Millisecond * 10):
			break
		default:
			break
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		c.cancel()
		if c.conn != nil {
			c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, ""))
			c.conn.Close()
		}
		close(c.done)
		close(c.ch)
		c.checkLastPingsStop()
	}
	return nil
}

// MaxErrorCount is the number of errors trying to connect to the event-api server before giving up
var MaxErrorCount = 50

var defaultInsecureWSDialer = &websocket.Dialer{
	Proxy:            http.ProxyFromEnvironment,
	HandshakeTimeout: 45 * time.Second,
	TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true,
	},
	EnableCompression: true,
}

func (c *SubscriptionChannel) pingReceived() {
	c.lastPingMu.Lock()
	c.lastPing = time.Now()
	c.lastPingMu.Unlock()
}

const expectedDurationBetweenPings = 30 * time.Second

func (c *SubscriptionChannel) checkLastPingsInit() {
	c.pingReceived() // make initial checks right
	c.lastPingTimer = time.NewTicker(expectedDurationBetweenPings)
	c.lastPingsDone = make(chan bool)
}

func (c *SubscriptionChannel) checkLastPingsLoop() {
	for {
		select {
		case <-c.lastPingsDone:
			return
		case <-c.lastPingTimer.C:
			c.lastPingMu.Lock()
			lastPing := c.lastPing
			c.lastPingMu.Unlock()
			// crash if 5 last pings were missed
			// would be better to retry subscription, but can't figure out how to do this cleanly here, since it would be blocked on wch.ReadJSON()
			if time.Since(lastPing) > 5*expectedDurationBetweenPings {
				panic(fmt.Errorf("no pings received for %v, expecting pings every %v", time.Since(lastPing), expectedDurationBetweenPings))
			}
		}
	}
}

func (c *SubscriptionChannel) checkLastPingsStop() {
	if c.lastPingTimer != nil {
		c.lastPingTimer.Stop()
		c.lastPingsDone <- true
	}
}

// Publish will send a message to the event api
func (c *SubscriptionChannel) Publish(event PublishEvent, options ...Option) error {
	ts := time.Now()
	id := pstrings.NewUUIDV4()
	config := &PublishConfig{
		Deadline:  time.Now().Add(time.Minute),
		Timestamp: time.Now(),
		Logger:    c.subscription.Logger,
	}
	for _, opt := range options {
		if err := opt(config); err != nil {
			return err
		}
	}
	headers := make(map[string]string)
	for k, v := range event.Headers {
		headers[k] = v
	}
	for k, v := range config.Header {
		headers[k] = v[0]
	}
	// create a channel for blocking until we receive the publish ack
	ch := make(chan struct{})
	c.writeMu.Lock()
	c.writeCallbacks[id] = &publishAck{ch, config.Async}
	c.writeMu.Unlock()
	if !config.Async {
		// make sure we cleanup no matter the result if we're not async
		defer func() {
			c.writeMu.Lock()
			delete(c.writeCallbacks, id)
			c.writeMu.Unlock()
		}()
	}
	var writeErr error
	var sentOK bool
	var i int
	for time.Now().Before(config.Deadline) {
		c.writeMu.Lock() // we can only have one writer at a time on the conn
		c.mu.Lock()      // we need to hold for the conn
		if err := c.conn.WriteJSON(map[string]interface{}{
			"id":     id,
			"action": "publish",
			"data": pjson.Stringify(payload{
				Type:      "json",
				Model:     event.Object.GetModelName(),
				Data:      pjson.Stringify(event.Object),
				Headers:   headers,
				Timestamp: config.Timestamp,
			}),
		}); err != nil {
			if config.Logger != nil {
				log.Debug(config.Logger, "error publishing message", "err", err, "msgid", id, "attempt", i+1)
			}
			writeErr = err
			if IsErrorRetryable(err) {
				c.mu.Unlock()
				c.writeMu.Unlock()
				i++
				time.Sleep(time.Millisecond * time.Duration(10*i)) // expotential backoff
				continue
			}
		}
		c.mu.Unlock()
		c.writeMu.Unlock()
		sentOK = writeErr == nil // only if we don't have an error
		break
	}
	if writeErr != nil {
		return writeErr
	}
	if !sentOK {
		if config.Logger != nil {
			log.Error(config.Logger, "deadline exceeded publishing message", "duration", time.Since(ts), "msgid", id, "model", event.Object.GetModelName())
		}
		return ErrDeadlineExceeded
	}
	if config.Logger != nil {
		log.Debug(config.Logger, "published message", "duration", time.Since(ts), "msgid", id, "async", config.Async, "model", event.Object.GetModelName())
	}
	// we have to now wait for the ACK in the case we're not async
	// since we put the channel in the map above, the reader will signal that
	// channel once the ACK is received and we'll block until we either
	// (A) get that channel notification or (B) time out
	if !config.Async {
		ts = time.Now()
		var timedout bool
		select {
		case <-ch:
			break
		case <-time.After(config.Deadline.Sub(time.Now())):
			timedout = true
			break
		}
		if timedout {
			if config.Logger != nil {
				log.Error(config.Logger, "deadline exceeded waiting for publish ack", "duration", time.Since(ts), "msgid", id, "model", event.Object.GetModelName())
			}
			return ErrDeadlineExceeded
		}
		if config.Logger != nil {
			log.Debug(config.Logger, "publish message ack", "duration", time.Since(ts), "msgid", id, "model", event.Object.GetModelName())
		}
	}
	return nil
}

func (c *SubscriptionChannel) run() {

	origin := api.BackendURL(api.EventService, c.subscription.Channel)
	if strings.HasSuffix(origin, "/") {
		origin = origin[0 : len(origin)-1]
	}
	u := strings.ReplaceAll(pstrings.JoinURL(origin, "ws"), "http://", "ws://")
	u = strings.ReplaceAll(u, "https://", "wss://")
	headers := make(http.Header)
	headers.Set("Origin", origin)
	if c.subscription.APIKey != "" {
		headers.Set(api.AuthorizationHeader, c.subscription.APIKey)
	}
	var dialer = websocket.DefaultDialer

	if strings.Contains(u, "ppoint.io") {
		headers.Set("pinpt-customer-id", "5500a5ba8135f296")            // test case, doesn't work for real except local
		headers.Set("x-api-key", "fa0s8f09a8sd09f8iasdlkfjalsfm,.m,xf") // test case, doesn't work for real except local
	}

	if strings.Contains(u, "host.docker.internal") || strings.Contains(u, ".svc.cluster.local") {
		// when running inside local docker or kubernetes, turn off TLS cert check
		dialer = defaultInsecureWSDialer
	}

	// provide ability to set HTTP headers on the subscription
	for k, v := range c.headers {
		headers.Set(k, v)
	}

	dispatchTimeout := c.subscription.DispatchTimeout
	if dispatchTimeout <= 0 {
		dispatchTimeout = time.Minute
	}

	var errors int

	for {
		c.mu.Lock()
		finished := c.closed
		c.mu.Unlock()
		if finished {
			break
		}
		wch, _, err := dialer.Dial(u, headers)
		if err != nil {
			var isRetryableError bool
			if IsErrorRetryable(err) {
				isRetryableError = true
			} else {
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
				log.Debug(c.subscription.Logger, "connection failed, will try again", "count", errors, "err", err)
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
		log.Debug(c.subscription.Logger, "connected")

		pinghandler := wch.PingHandler()
		wch.SetPingHandler(func(data string) error {
			c.pingReceived()
			return pinghandler(data)
		})

		var subid string

		if len(c.subscription.Topics) > 0 {
			subaction := action{
				ID:     pstrings.NewUUIDV4(),
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
			subid = subaction.ID
		} else {
			c.mu.Lock()
			if !c.isready {
				// only signal once
				c.isready = true
				c.ready <- true
			}
			c.mu.Unlock()
		}

		if !c.subscription.DisablePing {
			c.checkLastPingsInit() // have it separately to avoid races with Close
			go c.checkLastPingsLoop()
		}

		errors = 0
		var errored bool
		var closed bool
		var acked bool

		// now we just read messages until we're EOF
		for !closed {
			var actionresp actionResponse
			if err := wch.ReadJSON(&actionresp); err != nil {
				if IsErrorRetryable(err) || websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure, websocket.CloseTryAgainLater, websocket.CloseAbnormalClosure) {
					closed = true
					errored = true
					log.Debug(c.subscription.Logger, "connection has been closed, will try to reconnect", "err", err)
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
			if EventDebug {
				fmt.Println("[event-api] received event", "response", actionresp)
			}
			if subid == actionresp.ID {
				if !acked {
					// if the subscribe ack worked, great ... continue
					if actionresp.Success {
						acked = true
						c.mu.Lock()
						if !c.isready {
							// only signal once
							c.isready = true
							c.ready <- true
						}
						c.mu.Unlock()
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
			} else {
				c.writeMu.RLock()
				ackch := c.writeCallbacks[actionresp.ID]
				c.writeMu.RUnlock()
				if ackch != nil {
					if ackch.async {
						// delete if async
						c.writeMu.Lock()
						delete(c.writeCallbacks, actionresp.ID)
						c.writeMu.Unlock()
					} else {
						// signal the blocker
						ackch.ch <- struct{}{}
					}
				}
			}
			if actionresp.Data != nil {
				c.mu.Lock()
				if !c.closed {
					subdata := actionresp.Data
					if c.subscription.DisableAutoCommit {
						subdata.commitch = make(chan bool)
					}
					select {
					case c.ch <- *subdata:
					case <-time.After(dispatchTimeout):
						log.Warn(c.subscription.Logger, "dispatch timed out", "duration", dispatchTimeout, "data", actionresp.Data, "id", subid)
					}
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
	log.Debug(c.subscription.Logger, "disconnected")
}

// SubscriptionFilter are subscription related filters
type SubscriptionFilter struct {
	HeaderExpr string `json:"header,omitempty"`
	ObjectExpr string `json:"object,omitempty"`
}

// Subscription is the information for creating a subscription channel to receive events from the event server
type Subscription struct {
	GroupID           string              `json:"group_id"`
	Topics            []string            `json:"topics"`
	Headers           map[string]string   `json:"headers,omitempty"`
	IdleDuration      string              `json:"idle_duration,omitempty"` // Deprecated
	Limit             int                 `json:"limit,omitempty"`
	Offset            string              `json:"offset,omitempty"` // Deprecated
	After             int64               `json:"after,omitempty"`
	DisableAutoCommit bool                `json:"disable_autocommit,omitempty"`
	Temporary         bool                `json:"temporary,omitempty"`
	Filter            *SubscriptionFilter `json:"filter,omitempty"`
	Channel           string              `json:"-"`
	APIKey            string              `json:"-"`
	BufferSize        int                 `json:"-"`
	Errors            chan<- error        `json:"-"`
	Logger            log.Logger          `json:"-"`
	HTTPHeaders       map[string]string   `json:"-"`
	CloseTimeout      time.Duration       `json:"-"`
	DispatchTimeout   time.Duration       `json:"-"`
	DisablePing       bool                `json:"-"`
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
		ctx:            newctx,
		cancel:         cancel,
		ch:             make(chan SubscriptionEvent, subscription.BufferSize),
		done:           make(chan bool, 1),
		ready:          make(chan bool, 1),
		subscription:   subscription,
		headers:        subscription.HTTPHeaders,
		writeCallbacks: make(map[string]*publishAck),
	}
	go subch.run()
	return subch, nil
}
