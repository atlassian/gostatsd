package newrelic

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd"
	stats "github.com/atlassian/gostatsd/pkg/statser"

	"github.com/cenkalti/backoff"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	// BackendName is the name of this backend.
	BackendName                  = "newrelic"
	integrationName              = "com.newrelic.gostatsd"
	integrationVersion           = "2.0.0"
	protocolVersion              = "2"
	defaultUserAgent             = "gostatsd"
	defaultMaxRequestElapsedTime = 15 * time.Second
	defaultClientTimeout         = 9 * time.Second
	// defaultMetricsPerBatch is the default number of metrics to send in a single batch.
	defaultMetricsPerBatch = 1000
	// maxResponseSize is the maximum response size we are willing to read.
	maxResponseSize     = 10 * 1024
	maxConcurrentEvents = 20

	defaultEnableHttp2 = false
)

var (
	// defaultMaxRequests is the number of parallel outgoing requests to New Relic.  As this mixes both
	// CPU (JSON encoding, TLS) and network bound operations, balancing may require some experimentation.
	defaultMaxRequests = uint(2 * runtime.NumCPU())
)

// Client represents a New Relic client.
type Client struct {
	address   string
	eventType string
	flushType string
	tagPrefix string
	//Options to define your own field names to support other StatsD implementations
	metricName      string
	metricType      string
	metricPerSecond string
	metricValue     string
	timerMin        string
	timerMax        string
	timerCount      string
	timerMean       string
	timerMedian     string
	timerStdDev     string
	timerSum        string
	timerSumSquares string

	batchesCreated uint64 // Accumulated number of batches created
	batchesRetried uint64 // Accumulated number of batches retried (first send is not a retry)
	batchesDropped uint64 // Accumulated number of batches aborted (data loss)
	batchesSent    uint64 // Accumulated number of batches successfully sent

	userAgent             string
	maxRequestElapsedTime time.Duration
	client                http.Client
	metricsPerBatch       uint
	metricsBufferSem      chan *bytes.Buffer // Two in one - a semaphore and a buffer pool
	eventsBufferSem       chan *bytes.Buffer // Two in one - a semaphore and a buffer pool
	now                   func() time.Time   // Returns current time. Useful for testing.

	disabledSubtypes gostatsd.TimerSubtypes
	flushInterval    time.Duration
}

// NewRelicPayload struct
type NewRelicPayload struct {
	Name               string        `json:"name"`
	ProtocolVersion    string        `json:"protocol_version"`
	IntegrationVersion string        `json:"integration_version"`
	Data               []interface{} `json:"data"`
}

// SendMetricsAsync flushes the metrics to New Relic, preparing payload synchronously but doing the send asynchronously.
func (n *Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	counter := 0
	results := make(chan error)
	n.processMetrics(metrics, func(ts *timeSeries) {
		// This section would be likely be better if it pushed all ts's in to a single channel
		// which n goroutines then read from.  Current behavior still spins up many goroutines
		// and has them all hit the same channel.
		atomic.AddUint64(&n.batchesCreated, 1)
		go func() {
			select {
			case <-ctx.Done():
				return
			case buffer := <-n.metricsBufferSem:
				defer func() {
					buffer.Reset()
					n.metricsBufferSem <- buffer
				}()
				err := n.postMetrics(ctx, buffer, ts)

				select {
				case <-ctx.Done():
				case results <- err:
				}
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

// RunMetrics x
func (n *Client) RunMetrics(ctx context.Context, statser stats.Statser) {
	statser = statser.WithTags(gostatsd.Tags{"backend:newrelic"})

	flushed, unregister := statser.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushed:
			statser.Gauge("backend.created", float64(atomic.LoadUint64(&n.batchesCreated)), nil)
			statser.Gauge("backend.retried", float64(atomic.LoadUint64(&n.batchesRetried)), nil)
			statser.Gauge("backend.dropped", float64(atomic.LoadUint64(&n.batchesDropped)), nil)
			statser.Gauge("backend.sent", float64(atomic.LoadUint64(&n.batchesSent)), nil)
		}
	}
}

func (n *Client) processMetrics(metrics *gostatsd.MetricMap, cb func(*timeSeries)) {
	fl := flush{
		ts: &timeSeries{
			Metrics: make([]interface{}, 0, n.metricsPerBatch),
			// Metrics: make([]NewRelicPayload, 0, n.metricsPerBatch),
		},
		timestamp:        float64(n.now().Unix()),
		flushIntervalSec: n.flushInterval.Seconds(),
		metricsPerBatch:  n.metricsPerBatch,
		cb:               cb,
	}

	metrics.Gauges.Each(func(key, tagsKey string, g gostatsd.Gauge) {
		fl.addMetric(n, "gauge", g.Value, Metric{}.PerSecond, g.Hostname, g.Tags, key, g.Timestamp)
		fl.maybeFlush()
	})

	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		fl.addMetric(n, "counter", float64(counter.Value), counter.PerSecond, counter.Hostname, counter.Tags, key, counter.Timestamp)
		fl.maybeFlush()
	})

	metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		fl.addMetric(n, "set", float64(len(set.Values)), Metric{}.PerSecond, set.Hostname, set.Tags, key, set.Timestamp)
		fl.maybeFlush()
	})

	metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		fl.addTimerMetric(n, "timer", timer, tagsKey, key)
		fl.maybeFlush()
	})

	fl.finish()
}

func (n *Client) postMetrics(ctx context.Context, buffer *bytes.Buffer, ts *timeSeries) error {
	return n.post(ctx, buffer, "metrics", ts)
}

// SendEvent sends an event to New Relic.
func (n *Client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	return nil
}

// Name returns the name of the backend.
func (n *Client) Name() string {
	return BackendName
}

func (n *Client) post(ctx context.Context, buffer *bytes.Buffer, typeOfPost string, data interface{}) error {
	post, err := n.constructPost(ctx, buffer, typeOfPost, data)
	if err != nil {
		atomic.AddUint64(&n.batchesDropped, 1)
		return err
	}

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = n.maxRequestElapsedTime
	for {
		if err = post(); err == nil {
			atomic.AddUint64(&n.batchesSent, 1)
			return nil
		}

		next := b.NextBackOff()
		if next == backoff.Stop {
			atomic.AddUint64(&n.batchesDropped, 1)
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

		atomic.AddUint64(&n.batchesRetried, 1)
	}
}

func (n *Client) constructPost(ctx context.Context, buffer *bytes.Buffer, typeOfPost string, data interface{}) (func() error /*doPost*/, error) {

	NRPayload := setDefaultPayload()
	NRPayload.Data = append(NRPayload.Data, data)
	mJSON, err := json.Marshal(NRPayload)

	if err != nil {
		return nil, fmt.Errorf("[%s] unable to marshal %s: %v", BackendName, typeOfPost, err)
	}

	return func() error {
		headers := map[string]string{
			"Content-Type": "application/json",
			"User-Agent":   n.userAgent,
		}
		req, err := http.NewRequest("POST", n.address, bytes.NewBuffer(mJSON))
		if err != nil {
			return fmt.Errorf("unable to create http.Request: %v", err)
		}
		req = req.WithContext(ctx)
		for header, v := range headers {
			req.Header.Set(header, v)
		}
		resp, err := n.client.Do(req)
		if err != nil {
			return fmt.Errorf("error POSTing: %s", err.Error())
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
	}, nil

}

// NewClientFromViper returns a new New Relic client.
func NewClientFromViper(v *viper.Viper) (gostatsd.Backend, error) {
	nr := getSubViper(v, "datadog")
	nr.SetDefault("address", "http://localhost:8001/v1/data")
	nr.SetDefault("event_type", "StatsD")
	nr.SetDefault("flush_type", "http")
	nr.SetDefault("tag_prefix", "")
	nr.SetDefault("metric_name", "metric_name")
	nr.SetDefault("metric_type", "metric_type")
	nr.SetDefault("metric_per_second", "metric_per_second")
	nr.SetDefault("metric_value", "metric_value")
	nr.SetDefault("timer_min", "samples_min")
	nr.SetDefault("timer_max", "samples_max")
	nr.SetDefault("timer_count", "samples_count")
	nr.SetDefault("timer_mean", "samples_mean")
	nr.SetDefault("timer_median", "samples_median")
	nr.SetDefault("timer_std_dev", "samples_std_dev")
	nr.SetDefault("timer_sum", "samples_sum")
	nr.SetDefault("timer_sum_square", "samples_sum_squares")

	nr.SetDefault("metrics_per_batch", defaultMetricsPerBatch)
	nr.SetDefault("network", "tcp")
	nr.SetDefault("client_timeout", defaultClientTimeout)
	nr.SetDefault("max_request_elapsed_time", defaultMaxRequestElapsedTime)
	nr.SetDefault("max_requests", defaultMaxRequests)
	nr.SetDefault("enable-http2", defaultEnableHttp2)
	nr.SetDefault("user-agent", defaultUserAgent)

	return NewClient(
		nr.GetString("address"),
		nr.GetString("event_type"),
		nr.GetString("flush_type"),
		nr.GetString("tag_prefix"),
		nr.GetString("metric_name"),
		nr.GetString("metric_type"),
		nr.GetString("metric_per_second"),
		nr.GetString("metric_value"),
		nr.GetString("timer_min"),
		nr.GetString("timer_max"),
		nr.GetString("timer_count"),
		nr.GetString("timer_mean"),
		nr.GetString("timer_median"),
		nr.GetString("timer_std_dev"),
		nr.GetString("timer_sum"),
		nr.GetString("timer_sum_square"),
		nr.GetString("user-agent"),
		nr.GetString("network"),
		uint(nr.GetInt("metrics_per_batch")),
		uint(nr.GetInt("max_requests")),
		nr.GetBool("enable-http2"),
		nr.GetDuration("client_timeout"),
		nr.GetDuration("max_request_elapsed_time"),
		v.GetDuration("flush-interval"), // Main viper, not sub-viper
		gostatsd.DisabledSubMetrics(v),
	)
}

// NewClient returns a new New Relic client.
func NewClient(address, eventType, flushType, tagPrefix,
	metricName, metricType, metricPerSecond, metricValue,
	timerMin, timerMax, timerCount, timerMean, timerMedian, timerStdDev, timerSum, timerSumSquares,
	userAgent, network string, metricsPerBatch, maxRequests uint, enableHttp2 bool,
	clientTimeout, maxRequestElapsedTime, flushInterval time.Duration, disabled gostatsd.TimerSubtypes) (*Client, error) {

	if metricsPerBatch <= 0 {
		return nil, fmt.Errorf("[%s] metricsPerBatch must be positive", BackendName)
	}
	if clientTimeout <= 0 {
		return nil, fmt.Errorf("[%s] clientTimeout must be positive", BackendName)
	}
	if maxRequestElapsedTime <= 0 {
		return nil, fmt.Errorf("[%s] maxRequestElapsedTime must be positive", BackendName)
	}

	log.Infof("[%s] maxRequestElapsedTime=%s maxRequests=%d clientTimeout=%s metricsPerBatch=%d", BackendName, maxRequestElapsedTime, maxRequests, clientTimeout, metricsPerBatch)

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
	}
	if !enableHttp2 {
		// A non-nil empty map used in TLSNextProto to disable HTTP/2 support in client.
		// https://golang.org/doc/go1.6#http2
		transport.TLSNextProto = map[string](func(string, *tls.Conn) http.RoundTripper){}
	}

	metricsBufferSem := make(chan *bytes.Buffer, maxRequests)
	for i := uint(0); i < maxRequests; i++ {
		metricsBufferSem <- &bytes.Buffer{}
	}
	eventsBufferSem := make(chan *bytes.Buffer, maxConcurrentEvents)
	for i := uint(0); i < maxConcurrentEvents; i++ {
		eventsBufferSem <- &bytes.Buffer{}
	}
	return &Client{
		address:               address,
		eventType:             eventType,
		flushType:             flushType,
		tagPrefix:             tagPrefix,
		metricName:            metricName,
		metricType:            metricType,
		metricPerSecond:       metricPerSecond,
		metricValue:           metricValue,
		timerMin:              timerMin,
		timerMax:              timerMax,
		timerCount:            timerCount,
		timerMean:             timerMean,
		timerMedian:           timerMedian,
		timerStdDev:           timerStdDev,
		timerSum:              timerSum,
		timerSumSquares:       timerSumSquares,
		userAgent:             userAgent,
		maxRequestElapsedTime: maxRequestElapsedTime,
		client: http.Client{
			Transport: transport,
			Timeout:   clientTimeout,
		},
		metricsPerBatch:  metricsPerBatch,
		metricsBufferSem: metricsBufferSem,
		eventsBufferSem:  eventsBufferSem,
		now:              time.Now,
		flushInterval:    flushInterval,
		disabledSubtypes: disabled,
	}, nil
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	return n
}

func setDefaultPayload() NewRelicPayload {
	return NewRelicPayload{
		Name:               integrationName,
		ProtocolVersion:    protocolVersion,
		IntegrationVersion: integrationVersion,
	}
}
