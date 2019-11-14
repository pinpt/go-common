package upload

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/pinpt/go-common/api"
	"github.com/pinpt/httpclient"
)

// NOTE: these defaults are borrowed from https://github.com/aws/aws-sdk-go/blob/master/service/s3/s3manager/upload.go

// MaxUploadParts is the maximum allowed number of parts in a multi-part upload
// on Amazon S3.
const MaxUploadParts = 10000

// MinUploadPartSize is the minimum allowed part size when uploading a part to
// Amazon S3.
const MinUploadPartSize int64 = 1024 * 1024 * 5

// DefaultUploadPartSize is the default part size to buffer chunks of a
// payload into.
const DefaultUploadPartSize = MinUploadPartSize

// DefaultUploadConcurrency is the default number of goroutines to spin up when
// using Upload().
const DefaultUploadConcurrency = 5

// Options for configuring the upload
type Options struct {
	// APIKey is required for communication
	APIKey string

	// The buffer size (in bytes) to use when buffering data into chunks and
	// sending them as parts to S3. The minimum allowed part size is 5MB, and
	// if this value is set to zero, the DefaultUploadPartSize value will be used.
	PartSize int64

	// The number of goroutines to spin up in parallel per call to Upload when
	// sending parts. If this is set to zero, the DefaultUploadConcurrency value
	// will be used.
	//
	// The concurrency pool is not shared between calls to Upload.
	Concurrency int

	// Body is the content to upload. It's the responsibility of the caller to close this reader itself
	Body io.Reader

	// ContentType is the body content type
	ContentType string

	// URL to upload the parts to
	URL string

	// HTTPClientConfig is a custom httpclient.Config in case you want to override the behavior
	HTTPClientConfig *httpclient.Config
}

type part struct {
	index  int
	reader io.Reader
}

func newClient(opts Options) (httpclient.Client, error) {
	if opts.HTTPClientConfig == nil {
		opts.HTTPClientConfig = &httpclient.Config{
			Paginator: httpclient.NoPaginator(),
			Retryable: httpclient.NewBackoffRetry(10*time.Millisecond, 100*time.Millisecond, 30*time.Second, 2.0),
		}
	}
	return api.NewHTTPAPIClient(opts.HTTPClientConfig)
}

func upload(opts Options, urlpath string, reader io.Reader) error {
	client, err := newClient(opts)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, urlpath, reader)
	if err != nil {
		return err
	}
	api.SetAuthorization(req, opts.APIKey)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusAccepted {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		return nil
	}
	return fmt.Errorf("error uploading %v. status code was %v after multi attempts", urlpath, resp.StatusCode)
}

// Upload a file to the upload server in multi part upload
// returns the number of parts, the total size in bytes of the upload and an optional error (nil if none)
func Upload(opts Options) (int, int64, error) {
	if opts.PartSize <= 0 || opts.PartSize < MinUploadPartSize {
		opts.PartSize = DefaultUploadPartSize
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = DefaultUploadConcurrency
	}
	if opts.APIKey == "" {
		return 0, 0, fmt.Errorf("missing required APIKey")
	}
	if opts.Body == nil {
		return 0, 0, fmt.Errorf("missing required Body")
	}
	if opts.ContentType == "" {
		return 0, 0, fmt.Errorf("missing required ContentType")
	}
	if opts.URL == "" {
		return 0, 0, fmt.Errorf("missing required URL")
	}
	var wg sync.WaitGroup
	ch := make(chan part, opts.Concurrency)
	errors := make(chan error, opts.Concurrency)
	for i := 0; i < opts.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range ch {
				if err := upload(opts, fmt.Sprintf("%s.%d", opts.URL, r.index), r.reader); err != nil {
					errors <- err
					return
				}
			}
		}()
	}
	var index int
	var total int64
	// split the buffer in chunks of PartSize and upload each part separately
	// in its own goroutine
	for {
		buf := make([]byte, opts.PartSize)
		n, err := opts.Body.Read(buf)
		if err == io.EOF || n < 0 {
			break
		}
		if err != nil {
			return 0, 0, err
		}
		total += int64(n)
		var ok bool
		for !ok {
			select {
			case ch <- part{reader: bytes.NewReader(buf[:n]), index: index}:
				ok = true
				break
			default:
				// check to make sure not in an error state
				select {
				case err := <-errors:
					return 0, 0, err
				default:
				}
				time.Sleep(time.Microsecond)
			}
		}
		index++
	}
	close(ch)
	wg.Wait()
	select {
	case err := <-errors:
		return 0, 0, err
	default:
	}
	return index, total, nil
}
