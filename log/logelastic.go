package log

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	elastic "github.com/olivere/elastic"
	"github.com/pinpt/go-common/datetime"
)

type esrecord struct {
	Message   string                 `json:"message"`
	Level     string                 `json:"severity"`
	Timestamp string                 `json:"@timestamp"`
	Meta      map[string]interface{} `json:"fields"`
}

// esbulker is an example for a process that needs to push data into
// Elasticsearch via BulkProcessor.
type esbulker struct {
	c            *elastic.Client
	p            *elastic.BulkProcessor
	workers      int
	records      chan esrecord
	currentindex string        // only to know when we roll over
	stopC        chan struct{} // stop channel for the indexer
	throttleMu   sync.Mutex    // guards the following block
	throttle     bool          // throttle (or stop) sending data into bulk processor?
}

// Run starts the esbulker.
func (b *esbulker) Run() error {

	// set reasonable defaults
	batchSize := 1000
	bulkSize := 4096
	flushInternal := 10 * time.Second

	// allow env overrides
	batchSizeStr := os.Getenv("PP_ELASTIC_BATCH_SIZE")
	if batchSizeStr != "" {
		val, err := strconv.Atoi(batchSizeStr)
		if err != nil {
			return fmt.Errorf("error converting PP_ELASTIC_BATCH_SIZE to int: %v", err)
		}
		if val <= 0 {
			return fmt.Errorf("error converting PP_ELASTIC_BATCH_SIZE to int: value must be > 0")
		}
		batchSize = int(val)
	}
	bulkSizeStr := os.Getenv("PP_ELASTIC_BULK_SIZE")
	if bulkSizeStr != "" {
		val, err := strconv.Atoi(bulkSizeStr)
		if err != nil {
			return fmt.Errorf("error converting PP_ELASTIC_BULK_SIZE to int: %v", err)
		}
		if val <= 0 {
			return fmt.Errorf("error converting PP_ELASTIC_BULK_SIZE to int: value must be > 0")
		}
		bulkSize = int(val)
	}
	flushIntervalStr := os.Getenv("PP_ELASTIC_FLUSH_INTERVAL")
	if flushIntervalStr != "" {
		val, err := time.ParseDuration(flushIntervalStr)
		if err != nil {
			return fmt.Errorf("error converting PP_ELASTIC_FLUSH_INTERVAL to duration: %v", err)
		}
		if val <= 0 {
			return fmt.Errorf("error converting PP_ELASTIC_FLUSH_INTERVAL to duration: value must be > 0")
		}
		flushInternal = val
	}

	// Start bulk processor
	p, err := b.c.BulkProcessor().
		Workers(b.workers).           // # of workers
		BulkActions(batchSize).       // # of queued requests before committed
		BulkSize(bulkSize).           // # of bytes in requests before committed
		FlushInterval(flushInternal). // autocommit every N
		Do(context.Background())

	if err != nil {
		return err
	}

	b.p = p

	// create a channel to hold pending records
	b.records = make(chan esrecord, 2*batchSize)

	// Start indexer that pushes data into bulk processor
	b.stopC = make(chan struct{})
	go b.indexer()

	return nil
}

// Add a record to be queued for delivery
func (b *esbulker) Add(record esrecord) {
	b.records <- record
}

// Close the esbulker.
func (b *esbulker) Close() error {
	b.stopC <- struct{}{}
	<-b.stopC
	close(b.stopC)
	b.p.Close()
	return nil
}

func (b *esbulker) Flush() {
	b.p.Flush()
}

// indexer is a goroutine that periodically pushes data into
// bulk processor unless being "throttled" or "stopped".
func (b *esbulker) indexer() {
	var stop bool

	for !stop {
		select {
		case <-b.stopC:
			stop = true

		default:
			b.throttleMu.Lock()
			throttled := b.throttle
			b.throttleMu.Unlock()

			// if not throttled, add the record to the batch processor,
			// otherwise, keep it in the channel until we're unblocked
			if !throttled {
				select {
				case doc := <-b.records:
					index := time.Now().Format("logs-2006.01.02")
					// check the index and if first time or different, make sure we create it
					// in elastic (if not already exists)
					if b.currentindex != index {
						b.ensureIndex(context.Background(), index)
						b.currentindex = index
					}
					r := elastic.NewBulkIndexRequest().Index(index).Type("doc").Doc(doc)
					b.p.Add(r)
				default:
					break
				}
			}
		}
	}

	b.stopC <- struct{}{} // ack stopping
}

// ensureIndex creates the index in Elasticsearch.
// It will be dropped if it already exists.
func (b *esbulker) ensureIndex(ctx context.Context, index string) error {
	exists, err := b.c.IndexExists(index).Do(ctx)
	if err != nil {
		return err
	}
	if exists {
		_, err = b.c.DeleteIndex(index).Do(ctx)
		if err != nil {
			return err
		}
	}
	_, err = b.c.CreateIndex(index).Do(ctx)
	if err != nil {
		return err
	}
	return nil
}

type eslog struct {
	next     Logger
	esbulker *esbulker
	hostname string
}

func (l *eslog) Log(keyvals ...interface{}) error {
	if l.esbulker != nil {
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
		kv["service"] = l.hostname
		l.esbulker.Add(esrecord{
			Timestamp: datetime.ISODate(),
			Message:   msg,
			Level:     lvl,
			Meta:      kv,
		})
	}
	if l.next != nil {
		return l.next.Log(keyvals...)
	}
	return nil
}

func (l *eslog) Close() error {
	if l.esbulker != nil {
		esGlobalLock.Lock()
		defer esGlobalLock.Unlock()
		esClients-- // refcount, once we get to 0, release
		if esClients == 0 {
			l.esbulker.Close()
			l.esbulker.c.Stop()
		}
	}
	return nil
}

// make it a singleton since we can have a ton of logger instances
var esGlobalLock sync.Mutex
var esClients int
var esGlobalesbulker *esbulker

func getESGlobalesbulker() *esbulker {
	esGlobalLock.Lock()
	defer esGlobalLock.Unlock()
	if esGlobalesbulker == nil {
		url := os.Getenv("PP_ELASTIC_URL")
		if url == "" {
			panic("missing env PP_ELASTIC_URL")
		}
		username := os.Getenv("PP_ELASTIC_USERNAME")
		if username == "" {
			panic("missing env PP_ELASTIC_USERNAME")
		}
		password := os.Getenv("PP_ELASTIC_PASSWORD")
		if password == "" {
			panic("missing env PP_ELASTIC_PASSWORD")
		}
		client, err := elastic.NewClient(elastic.SetURL(url), elastic.SetBasicAuth(username, password), elastic.SetSniff(false))
		if err != nil {
			panic("error creating elastic client: " + err.Error())
		}
		workerStr := os.Getenv("PP_ELASTIC_WORKERS")
		if workerStr == "" || workerStr == "0" {
			workerStr = "2"
		}
		workers, err := strconv.ParseInt(workerStr, 32, 10)
		if err != nil {
			panic("error parsing PP_ELASTIC_WORKERS: " + err.Error())
		}
		esGlobalesbulker = &esbulker{
			c:       client,
			workers: int(workers),
		}
		if err := esGlobalesbulker.Run(); err != nil {
			panic("error starting the elastic bulk processor: " + err.Error())
		}
	}
	esClients++ // ref count
	return esGlobalesbulker
}

// newESLogger returns a elastic logger
func newESLogger(next Logger) LoggerCloser {
	url := os.Getenv("PP_ELASTIC_URL")
	if url != "" {
		return &eslog{
			next:     next,
			esbulker: getESGlobalesbulker(),
			hostname: os.Getenv("PP_HOSTNAME"),
		}
	}
	return &eslog{
		next: next,
	}
}
