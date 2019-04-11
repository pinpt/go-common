package log

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const logdnaBaseURL = "https://logs.logdna.com/logs/ingest"
const maxNumLines = 100

type payload struct {
	Lines []line `json:"lines"`

	mu         *sync.RWMutex
	bufferSize uint32
}

// Flush payload
func (p *payload) Flush() {
	p.mu.Lock()
	p.Lines = []line{}
	p.mu.Unlock()
}

func (p *payload) Write(l line) bool {
	p.mu.Lock()
	p.Lines = append(p.Lines, l)
	readytosend := len(p.Lines) >= maxNumLines
	p.mu.Unlock()
	return readytosend
}

func (p *payload) Size() uint32 {
	p.mu.RLock()
	size := len(p.Lines)
	p.mu.RUnlock()
	return uint32(size)
}

type line struct {
	Timestamp int64                  `json:"timestamp"`
	Line      string                 `json:"line"`
	App       string                 `json:"app"`
	Level     string                 `json:"level,omitempty"`
	Env       string                 `json:"env,omitempty"`
	Meta      map[string]interface{} `json:"meta,omitempty"`
}

type client struct {
	apikey   string
	hostname string
	mac      string
	ip       string
	tags     []string
	url      string

	monitor *monitor

	mu      sync.Mutex
	payload *payload
}

func (c *client) send(force bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !force && c.payload.Size() == 0 {
		return
	}
	c.payload.mu.Lock()
	defer func() {
		c.payload.mu.Unlock()
		c.payload.Flush()
	}()
	body, err := json.Marshal(c.payload)
	if err != nil {
		fmt.Println("Error marshaling logdna payload", err)
		return
	}
	apiurl, _ := url.Parse(c.url)
	apiurl.User = url.User(c.apikey)
	qs := url.Values{}
	qs.Set("hostname", c.hostname)
	qs.Set("mac", c.mac)
	qs.Set("ip", c.ip)
	qs.Set("now", strconv.FormatInt(time.Now().UnixNano()/1000000, 10))
	qs.Set("tags", strings.Join(c.tags, ","))
	apiurl.RawQuery = qs.Encode()

	resp, err := http.Post(apiurl.String(), "application/json", bytes.NewReader(body))
	if err != nil {
		fmt.Println("error constructing logdna url", err)
		return
	}
	defer resp.Body.Close()
	// read error once get unexpected HTTP status code
	if resp.StatusCode >= 400 {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("error reading logdna response body", err)
		} else {
			fmt.Println("error making logdna injest request", string(b))
		}
	}
}

type dnalog struct {
	next   Logger
	client *client
}

func (l *dnalog) Log(keyvals ...interface{}) error {
	if l.client != nil {
		var msg string
		lvl := "info"
		kv := make(map[string]interface{})
		for i, val := range keyvals {
			valstr := fmt.Sprintf("%v", val)
			switch valstr {
			case "msg":
				msg = keyvals[i+1].(string)
				break
			case "level":
				lvl = fmt.Sprintf("%v", keyvals[i+1])
				break
			default:
				if i%2 == 0 {
					kv[valstr] = keyvals[i+1]
				}
			}
		}
		if readytosend := l.client.payload.Write(line{
			Timestamp: time.Now().UnixNano() / 1000000,
			App:       l.client.hostname,
			Line:      msg,
			Level:     lvl,
			Meta:      kv,
		}); readytosend {
			go l.client.send(false)
		}
	}
	if l.next != nil {
		return l.next.Log(keyvals...)
	}
	return nil
}

func (l *dnalog) Close() error {
	if l.client != nil {
		l.client.send(true)
		l.client.monitor.done <- struct{}{}
		l.client = nil
	}
	return nil
}

func getAddr() (ip string, addr string) {
	interfaces, err := net.Interfaces()
	if err == nil {
		for _, i := range interfaces {
			if i.Flags&net.FlagUp != 0 && bytes.Compare(i.HardwareAddr, nil) != 0 {
				// Don't use random as we have a real address
				addrs, _ := i.Addrs()
				if addrs != nil && len(addrs) > 0 {
					ip = addrs[0].String()
				}
				addr = i.HardwareAddr.String()
				break
			}
		}
	}
	return
}

type monitor struct {
	client *client
	done   chan struct{}
}

func (m *monitor) run() {
	for {
		select {
		case <-time.After(time.Minute):
			m.client.send(true)
		case <-m.done:
			return
		default:
		}
	}
}

// newDNALogger returns a log dna logger
func newDNALogger(next Logger) LoggerCloser {
	var c *client
	apikey := os.Getenv("PP_LOG_KEY")
	if apikey != "" {
		hostname := os.Getenv("PP_HOSTNAME")
		if hostname == "" {
			hostname = "hostname.not.provided"
		}
		tags := []string{}
		tagstr := os.Getenv("PP_LOG_TAGS")
		if tagstr != "" {
			tags = strings.Split(tagstr, ",")
		}
		logurl := logdnaBaseURL
		logurlstr := os.Getenv("PP_LOG_URL")
		if logurlstr != "" {
			logurl = logurlstr
		}
		ip, mac := getAddr()
		c = &client{
			apikey:   apikey,
			hostname: hostname,
			mac:      mac,
			ip:       ip,
			tags:     tags,
			url:      logurl,
			payload: &payload{
				mu: &sync.RWMutex{},
			},
		}
		m := &monitor{
			client: c,
			done:   make(chan struct{}, 1),
		}
		c.monitor = m
		go m.run()
	}
	return &dnalog{
		next:   next,
		client: c,
	}
}
