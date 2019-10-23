package upload

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"
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

// MaxAttemptsForPart is the max number of attempts for a part before failing
const MaxAttemptsForPart = 10

// Options for configuring the upload
type Options struct {
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

	// AttemptsForPart is the max number of attempts to retry (if failed) before
	// given up. Defaults to MaxAttemptsForPart
	AttemptsForPart int

	// Headers is the headers that are set on each outbound request. must be in the format name: value
	Headers []string

	// Body is the content to upload
	Body io.ReadCloser

	// ContentType is the body content type
	ContentType string

	// URL to upload the parts to
	URL string

	// Job information about the upload which will be saved into a file named job.json in the same folder
	Job map[string]interface{}
}

type part struct {
	index  int
	reader io.Reader
}

func newClient() (httpclient.Client, error) {
	hcConfig := &httpclient.Config{
		Paginator: httpclient.NoPaginator(),
		Retryable: httpclient.NewBackoffRetry(10*time.Millisecond, 100*time.Millisecond, 30*time.Second, 2.0),
	}
	return api.NewHTTPAPIClient(hcConfig)
}

func upload(opts Options, urlpath string, reader io.Reader) error {
	client, err := newClient()
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, urlpath, reader)
	if err != nil {
		return err
	}
	for _, header := range opts.Headers {
		tok := strings.Split(header, ": ")
		req.Header.Set(tok[0], strings.TrimSpace(tok[1]))
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
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
	if len(opts.Headers) == 0 {
		return 0, 0, fmt.Errorf("missing required Headers")
	}
	if opts.AttemptsForPart <= 0 || opts.AttemptsForPart > MaxAttemptsForPart {
		opts.AttemptsForPart = MaxAttemptsForPart
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
			case ch <- part{reader: bytes.NewBuffer(buf[:n]), index: index}:
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
	opts.Body.Close() // close the body after done
	select {
	case err := <-errors:
		return 0, 0, err
	default:
	}
	// upload the job if we have one once we're done
	if opts.Job != nil {
		u, _ := url.Parse(opts.URL)
		u.Path = path.Join(path.Dir(u.Path), "job.json")
		buf, err := json.Marshal(opts.Job)
		if err != nil {
			return 0, 0, fmt.Errorf("error serializing the job: %v", err)
		}
		if err := upload(opts, u.String(), bytes.NewReader(buf)); err != nil {
			return 0, 0, fmt.Errorf("error uploading job.json: %v", err)
		}
	}
	return index, total, nil
}