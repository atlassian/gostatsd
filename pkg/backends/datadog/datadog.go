package datadog

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd"
	stats "github.com/atlassian/gostatsd/pkg/statser"
	jsoniter "github.com/json-iterator/go"

	"github.com/cenkalti/backoff"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	apiURL = "https://app.datadoghq.com"
	// BackendName is the name of this backend.
	BackendName                  = "datadog"
	dogstatsdVersion             = "5.6.3"
	dogstatsdUserAgent           = "python-requests/2.6.0 CPython/2.7.10"
	defaultMaxRequestElapsedTime = 15 * time.Second
	defaultClientTimeout         = 9 * time.Second
	// defaultMetricsPerBatch is the default number of metrics to send in a single batch.
	defaultMetricsPerBatch = 1000
	// maxResponseSize is the maximum response size we are willing to read.
	maxResponseSize = 10 * 1024
)

// defaultMaxRequests is the number of parallel outgoing requests to Datadog.  As this mixes both
// CPU (JSON encoding, TLS) and network bound operations, balancing may require some experimentation.
var defaultMaxRequests = uint(2 * runtime.NumCPU())

// Client represents a Datadog client.
type Client struct {
	apiKey                string
	apiEndpoint           string
	maxRequestElapsedTime time.Duration
	client                http.Client
	metricsPerBatch       uint
	requestSem            chan struct{}
	now                   func() time.Time // Returns current time. Useful for testing.
	compressPayload       bool

	batchesCreated uint64 // Accumulated number of batches created
	batchesRetried uint64 // Accumulated number of batches retried (first send is not a retry)
	batchesDropped uint64 // Accumulated number of batches aborted (data loss)
	batchesSent    uint64 // Accumulated number of batches successfully sent
}

// event represents an event data structure for Datadog.
type event struct {
	Title          string   `json:"title"`
	Text           string   `json:"text"`
	DateHappened   int64    `json:"date_happened,omitempty"`
	Hostname       string   `json:"host,omitempty"`
	AggregationKey string   `json:"aggregation_key,omitempty"`
	SourceTypeName string   `json:"source_type_name,omitempty"`
	Tags           []string `json:"tags,omitempty"`
	Priority       string   `json:"priority,omitempty"`
	AlertType      string   `json:"alert_type,omitempty"`
}

// SendMetricsAsync flushes the metrics to Datadog, preparing payload synchronously but doing the send asynchronously.
func (d *Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	counter := 0
	results := make(chan error)
	d.processMetrics(metrics, func(ts *timeSeries) {
		// This section would be likely be better if it pushed all ts's in to a single channel
		// which n goroutines then read from.  Current behavior still spins up many goroutines
		// and has them all hit the same channel.
		atomic.AddUint64(&d.batchesCreated, 1)
		go func() {
			select {
			case <-ctx.Done():
				return
			case d.requestSem <- struct{}{}:
				defer func() {
					<-d.requestSem
				}()
			}

			err := d.postMetrics(ctx, ts)

			select {
			case <-ctx.Done():
			case results <- err:
			}
		}()
		counter++
	})
	go func() {
		errs := make([]error, 0, counter)
	loop:
		for c := 0; c < counter; c++ {
			select {
			case <-ctx.Done():
				errs = append(errs, ctx.Err())
				break loop
			case err := <-results:
				errs = append(errs, err)
			}
		}
		cb(errs)
	}()
}

func (d *Client) RunMetrics(ctx context.Context, statser stats.Statser) {
	statser = statser.WithTags(gostatsd.Tags{"backend:datadog"})

	flushed, unregister := statser.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushed:
			statser.Gauge("backend.created", float64(atomic.LoadUint64(&d.batchesCreated)), nil)
			statser.Gauge("backend.retried", float64(atomic.LoadUint64(&d.batchesRetried)), nil)
			statser.Gauge("backend.dropped", float64(atomic.LoadUint64(&d.batchesDropped)), nil)
			statser.Gauge("backend.sent", float64(atomic.LoadUint64(&d.batchesSent)), nil)
		}
	}
}

func (d *Client) processMetrics(metrics *gostatsd.MetricMap, cb func(*timeSeries)) {
	fl := flush{
		ts: &timeSeries{
			Series: make([]metric, 0, d.metricsPerBatch),
		},
		timestamp:        float64(d.now().Unix()),
		flushIntervalSec: metrics.FlushInterval.Seconds(),
		metricsPerBatch:  d.metricsPerBatch,
		cb:               cb,
	}

	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		fl.addMetric(rate, counter.PerSecond, counter.Hostname, counter.Tags, key)
		fl.addMetricf(gauge, float64(counter.Value), counter.Hostname, counter.Tags, "%s.count", key)
		fl.maybeFlush()
	})

	metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		fl.addMetricf(gauge, timer.Min, timer.Hostname, timer.Tags, "%s.lower", key)
		fl.addMetricf(gauge, timer.Max, timer.Hostname, timer.Tags, "%s.upper", key)
		fl.addMetricf(gauge, float64(timer.Count), timer.Hostname, timer.Tags, "%s.count", key)
		fl.addMetricf(rate, timer.PerSecond, timer.Hostname, timer.Tags, "%s.count_ps", key)
		fl.addMetricf(gauge, timer.Mean, timer.Hostname, timer.Tags, "%s.mean", key)
		fl.addMetricf(gauge, timer.Median, timer.Hostname, timer.Tags, "%s.median", key)
		fl.addMetricf(gauge, timer.StdDev, timer.Hostname, timer.Tags, "%s.std", key)
		fl.addMetricf(gauge, timer.Sum, timer.Hostname, timer.Tags, "%s.sum", key)
		fl.addMetricf(gauge, timer.SumSquares, timer.Hostname, timer.Tags, "%s.sum_squares", key)
		for _, pct := range timer.Percentiles {
			fl.addMetricf(gauge, pct.Float, timer.Hostname, timer.Tags, "%s.%s", key, pct.Str)
		}
		fl.maybeFlush()
	})

	metrics.Gauges.Each(func(key, tagsKey string, g gostatsd.Gauge) {
		fl.addMetric(gauge, g.Value, g.Hostname, g.Tags, key)
		fl.maybeFlush()
	})

	metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		fl.addMetric(gauge, float64(len(set.Values)), set.Hostname, set.Tags, key)
		fl.maybeFlush()
	})

	fl.finish()
}

func (d *Client) postMetrics(ctx context.Context, ts *timeSeries) error {
	return d.post(ctx, "/api/v1/series", "metrics", ts)
}

// SendEvent sends an event to Datadog.
func (d *Client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	return d.post(ctx, "/api/v1/events", "events", &event{
		Title:          e.Title,
		Text:           e.Text,
		DateHappened:   e.DateHappened,
		Hostname:       e.Hostname,
		AggregationKey: e.AggregationKey,
		SourceTypeName: e.SourceTypeName,
		Tags:           e.Tags,
		Priority:       e.Priority.StringWithEmptyDefault(),
		AlertType:      e.AlertType.StringWithEmptyDefault(),
	})
}

// Name returns the name of the backend.
func (d *Client) Name() string {
	return BackendName
}

func (d *Client) post(ctx context.Context, path, typeOfPost string, data interface{}) error {
	tsBytes, err := jsoniter.Marshal(data)
	if err != nil {
		return fmt.Errorf("[%s] unable to marshal %s: %v", BackendName, typeOfPost, err)
	}
	log.Debugf("[%s] %s json: %s", BackendName, typeOfPost, tsBytes)

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = d.maxRequestElapsedTime
	authenticatedURL := d.authenticatedURL(path)

	// Selectively compress payload based on knowledge of whether the endpoint supports deflate encoding.
	// The metrics endpoint does, the events endpoint does not.
	compressPayload := false
	if d.compressPayload && typeOfPost == "metrics" {
		compressPayload = true
	}

	post := d.constructPost(ctx, authenticatedURL, tsBytes, compressPayload)
	for {
		if err = post(); err == nil {
			atomic.AddUint64(&d.batchesSent, 1)
			return nil
		}

		next := b.NextBackOff()
		if next == backoff.Stop {
			atomic.AddUint64(&d.batchesDropped, 1)
			return fmt.Errorf("[%s] %v", BackendName, err)
		}

		log.Warnf("[%s] failed to send %s, sleeping for %s: %v", BackendName, typeOfPost, next, err)

		timer := time.NewTimer(next)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		atomic.AddUint64(&d.batchesRetried, 1)
	}
}

func (d *Client) constructPost(ctx context.Context, authenticatedURL string, body []byte, compressPayload bool) func() error {
	if compressPayload {
		// Use deflate (zlib, since DD requires the zlib headers)
		compressed, err := deflate(body)
		if err != nil {
			return func() error { return err }
		}
		body = compressed
	}

	return func() error {
		headers := map[string]string{
			"Content-Type": "application/json",
			// Mimic dogstatsd code
			"DD-Dogstatsd-Version": dogstatsdVersion,
			"User-Agent":           dogstatsdUserAgent,
		}
		if compressPayload {
			headers["Content-Encoding"] = "deflate"
		}
		req, err := http.NewRequest("POST", authenticatedURL, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("unable to create http.Request: %v", err)
		}
		req = req.WithContext(ctx)
		for header, v := range headers {
			req.Header.Set(header, v)
		}
		resp, err := d.client.Do(req)
		if err != nil {
			return fmt.Errorf("error POSTing: %s", strings.Replace(err.Error(), d.apiKey, "*****", -1))
		}
		defer resp.Body.Close()
		body := io.LimitReader(resp.Body, maxResponseSize)
		if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusNoContent {
			b, _ := ioutil.ReadAll(body)
			log.Infof("[%s] failed request status: %d\n%s", BackendName, resp.StatusCode, b)
			return fmt.Errorf("received bad status code %d", resp.StatusCode)
		}
		_, _ = io.Copy(ioutil.Discard, body)
		return nil
	}
}

func (d *Client) authenticatedURL(path string) string {
	q := url.Values{
		"api_key": []string{d.apiKey},
	}
	return fmt.Sprintf("%s%s?%s", d.apiEndpoint, path, q.Encode())
}

// NewClientFromViper returns a new Datadog API client.
func NewClientFromViper(v *viper.Viper) (gostatsd.Backend, error) {
	dd := getSubViper(v, "datadog")
	dd.SetDefault("api_endpoint", apiURL)
	dd.SetDefault("metrics_per_batch", defaultMetricsPerBatch)
	dd.SetDefault("compress_payload", true)
	dd.SetDefault("network", "tcp")
	dd.SetDefault("client_timeout", defaultClientTimeout)
	dd.SetDefault("max_request_elapsed_time", defaultMaxRequestElapsedTime)
	dd.SetDefault("max_requests", defaultMaxRequests)

	return NewClient(
		dd.GetString("api_endpoint"),
		dd.GetString("api_key"),
		dd.GetString("network"),
		uint(dd.GetInt("metrics_per_batch")),
		uint(dd.GetInt("max_requests")),
		dd.GetBool("compress_payload"),
		dd.GetDuration("client_timeout"),
		dd.GetDuration("max_request_elapsed_time"),
	)
}

// NewClient returns a new Datadog API client.
func NewClient(apiEndpoint, apiKey, network string, metricsPerBatch, maxRequests uint, compressPayload bool, clientTimeout, maxRequestElapsedTime time.Duration) (*Client, error) {
	if apiEndpoint == "" {
		return nil, fmt.Errorf("[%s] apiEndpoint is required", BackendName)
	}
	if apiKey == "" {
		return nil, fmt.Errorf("[%s] apiKey is required", BackendName)
	}
	if metricsPerBatch <= 0 {
		return nil, fmt.Errorf("[%s] metricsPerBatch must be positive", BackendName)
	}
	if clientTimeout <= 0 {
		return nil, fmt.Errorf("[%s] clientTimeout must be positive", BackendName)
	}
	if maxRequestElapsedTime <= 0 {
		return nil, fmt.Errorf("[%s] maxRequestElapsedTime must be positive", BackendName)
	}

	log.Infof("[%s] maxRequestElapsedTime=%s maxRequests=%d clientTimeout=%s metricsPerBatch=%d compressPayload=%t", BackendName, maxRequestElapsedTime, maxRequests, clientTimeout, metricsPerBatch, compressPayload)

	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSHandshakeTimeout: 3 * time.Second,
		TLSClientConfig: &tls.Config{
			// Can't use SSLv3 because of POODLE and BEAST
			// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
			// Can't use TLSv1.1 because of RC4 cipher usage
			MinVersion: tls.VersionTLS12,
		},
		DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
			// replace the network with our own
			return dialer.DialContext(ctx, network, address)
		},
		MaxIdleConns:    50,
		IdleConnTimeout: 1 * time.Minute,
		// A non-nil empty map used in TLSNextProto to disable HTTP/2 support in client.
		// https://golang.org/doc/go1.6#http2
		TLSNextProto: map[string](func(string, *tls.Conn) http.RoundTripper){},
	}
	return &Client{
		apiKey:                apiKey,
		apiEndpoint:           apiEndpoint,
		maxRequestElapsedTime: maxRequestElapsedTime,
		client: http.Client{
			Transport: transport,
			Timeout:   clientTimeout,
		},
		metricsPerBatch: metricsPerBatch,
		requestSem:      make(chan struct{}, maxRequests),
		compressPayload: compressPayload,
		now:             time.Now,
	}, nil
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	return n
}

func deflate(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	compressor, err := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	if err != nil {
		return nil, fmt.Errorf("unable to create zlib writer: %v", err)
	}
	_, err = compressor.Write(body)
	if err != nil {
		return nil, fmt.Errorf("unable to write compressed payload: %v", err)
	}
	err = compressor.Close()
	if err != nil {
		return nil, fmt.Errorf("unable to close compressor: %v", err)
	}
	sc := buf.Len()
	sp := len(body)
	log.Debugf("payload_size=%d, compressed_size=%d, compression_ration=%.3f", sp, sc, float32(sc)/float32(sp))
	return buf.Bytes(), nil
}
