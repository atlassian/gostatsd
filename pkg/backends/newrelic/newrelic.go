package newrelic

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"

	"github.com/cenkalti/backoff"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	// BackendName is the name of this backend.
	BackendName                  = "newrelic"
	integrationName              = "com.newrelic.gostatsd"
	integrationVersion           = "2.1.0"
	protocolVersion              = "2"
	defaultUserAgent             = "gostatsd"
	defaultMaxRequestElapsedTime = 15 * time.Second
	defaultClientTimeout         = 9 * time.Second
	// defaultMetricsPerBatch is the default number of metrics to send in a single batch.
	defaultMetricsPerBatch = 1000
	// maxResponseSize is the maximum response size we are willing to read.
	maxResponseSize = 10 * 1024

	defaultEnableHttp2 = false
	flushTypeInsights  = "insights"
	flushTypeInfra     = "infra"
)

var (
	// Available flushTypes
	flushTypes = []string{flushTypeInsights, flushTypeInfra}

	// defaultMaxRequests is the number of parallel outgoing requests to New Relic.  As this mixes both
	// CPU (JSON encoding, TLS) and network bound operations, balancing may require some experimentation.
	defaultMaxRequests = uint(2 * runtime.NumCPU())
)

// Client represents a New Relic client.
type Client struct {
	address   string
	eventType string
	flushType string
	apiKey    string
	tagPrefix string
	// Options to define your own field names to support other StatsD implementations
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
	now                   func() time.Time   // Returns current time. Useful for testing.

	disabledSubtypes gostatsd.TimerSubtypes
	flushInterval    time.Duration
}

// NewRelicInfraPayload struct
type NewRelicInfraPayload struct {
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
				err := n.post(ctx, buffer, ts)

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
		},
		timestamp:        float64(n.now().Unix()),
		flushIntervalSec: n.flushInterval.Seconds(),
		metricsPerBatch:  n.metricsPerBatch,
		cb:               cb,
	}

	metrics.Gauges.Each(func(key, tagsKey string, g gostatsd.Gauge) {
		fl.addMetric(n, "gauge", g.Value, 0, g.Hostname, g.Tags, key, g.Timestamp)
		fl.maybeFlush()
	})

	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		fl.addMetric(n, "counter", float64(counter.Value), counter.PerSecond, counter.Hostname, counter.Tags, key, counter.Timestamp)
		fl.maybeFlush()
	})

	metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		fl.addMetric(n, "set", float64(len(set.Values)), 0, set.Hostname, set.Tags, key, set.Timestamp)
		fl.maybeFlush()
	})

	metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		fl.addTimerMetric(n, "timer", timer, tagsKey, key)
		fl.maybeFlush()
	})

	fl.finish()
}

// SendEvent sends an event to New Relic.
func (n *Client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	return nil
}

// Name returns the name of the backend.
func (n *Client) Name() string {
	return BackendName
}

func (n *Client) post(ctx context.Context, buffer *bytes.Buffer, data interface{}) error {
	post, err := n.constructPost(ctx, buffer, data)
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

		log.Warnf("[%s] failed to send, sleeping for %s: %v", BackendName, next, err)

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

func (n *Client) constructPost(ctx context.Context, buffer *bytes.Buffer, data interface{}) (func() error /*doPost*/, error) {

	var mJSON []byte
	var mErr error
	switch n.flushType {
	case flushTypeInsights:
		NRPayload := data.(*timeSeries).Metrics
		mJSON, mErr = json.Marshal(NRPayload)
	default:
		NRPayload := newInfraPayload(data)
		mJSON, mErr = json.Marshal(NRPayload)
	}

	if mErr != nil {
		return nil, fmt.Errorf("[%s] unable to marshal: %v", BackendName, mErr)
	}

	return func() error {
		headers := map[string]string{
			"Content-Type": "application/json",
			"User-Agent":   n.userAgent,
		}

		// Insights Event API requires gzip or deflate compression
		if n.flushType == flushTypeInsights && n.apiKey != "" {
			headers["X-Insert-Key"] = n.apiKey
			headers["Content-Encoding"] = "deflate"
			var b bytes.Buffer
			w := zlib.NewWriter(&b)
			w.Write(mJSON)
			w.Close()
			mJSON = b.Bytes()
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
			log.WithFields(log.Fields{
				"status": resp.StatusCode,
				"body":   b,
			}).Infof("[%s] failed request", BackendName)
			return fmt.Errorf("received bad status code %d", resp.StatusCode)
		}
		_, _ = io.Copy(ioutil.Discard, body)
		return nil
	}, nil

}

// NewClientFromViper returns a new New Relic client.
func NewClientFromViper(v *viper.Viper) (gostatsd.Backend, error) {
	nr := getSubViper(v, "newrelic")
	nr.SetDefault("address", "http://localhost:8001/v1/data")
	nr.SetDefault("event-type", "GoStatsD")
	if strings.Contains(nr.GetString("address"), "insights-collector.newrelic.com") {
		nr.SetDefault("flush-type", flushTypeInsights)
	} else {
		nr.SetDefault("flush-type", flushTypeInfra)
	}
	nr.SetDefault("api-key", "")
	nr.SetDefault("tag-prefix", "")
	nr.SetDefault("metric-name", "name")
	nr.SetDefault("metric-type", "type")
	nr.SetDefault("per-second", "per_second")
	nr.SetDefault("value", "value")
	nr.SetDefault("timer-min", "min")
	nr.SetDefault("timer-max", "max")
	nr.SetDefault("timer-count", "count")
	nr.SetDefault("timer-mean", "mean")
	nr.SetDefault("timer-median", "median")
	nr.SetDefault("timer-stddev", "std_dev")
	nr.SetDefault("timer-sum", "sum")
	nr.SetDefault("timer-sumsquare", "sum_squares")

	nr.SetDefault("metrics-per-batch", defaultMetricsPerBatch)
	nr.SetDefault("network", "tcp")
	nr.SetDefault("client-timeout", defaultClientTimeout)
	nr.SetDefault("max-request-elapsed-time", defaultMaxRequestElapsedTime)
	nr.SetDefault("max-requests", defaultMaxRequests)
	nr.SetDefault("enable-http2", defaultEnableHttp2)
	nr.SetDefault("user-agent", defaultUserAgent)

	// New Relic Config Defaults & Recommendations
	v.SetDefault("statser-type", "null")
	v.SetDefault("flush-interval", "10s")
	if v.GetString("statser-type") == "null" {
		log.Infof("[%s] internal metrics OFF, to enable set 'statser-type' to 'logging' or 'internal'", BackendName)
	}

	return NewClient(
		nr.GetString("address"),
		nr.GetString("event-type"),
		nr.GetString("flush-type"),
		nr.GetString("api-key"),
		nr.GetString("tag-prefix"),
		nr.GetString("metric-name"),
		nr.GetString("metric-type"),
		nr.GetString("per-second"),
		nr.GetString("value"),
		nr.GetString("timer-min"),
		nr.GetString("timer-max"),
		nr.GetString("timer-count"),
		nr.GetString("timer-mean"),
		nr.GetString("timer-median"),
		nr.GetString("timer-stddev"),
		nr.GetString("timer-sum"),
		nr.GetString("timer-sumsquare"),
		nr.GetString("user-agent"),
		nr.GetString("network"),
		uint(nr.GetInt("metrics-per-batch")),
		uint(nr.GetInt("max-requests")),
		nr.GetBool("enable-http2"),
		nr.GetDuration("client-timeout"),
		nr.GetDuration("max-request-elapsed-time"),
		v.GetDuration("flush-interval"), // Main viper, not sub-viper
		gostatsd.DisabledSubMetrics(v),
	)
}

// NewClient returns a new New Relic client.
func NewClient(address, eventType, flushType, apiKey, tagPrefix,
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
	if !contains(flushTypes, flushType) && flushType != "" {
		return nil, fmt.Errorf("[%s] flushType (%s) is not supported", BackendName, flushType)
	}
	if flushType == flushTypeInsights && apiKey == "" {
		return nil, fmt.Errorf("[%s] api-key is required to flush to insights", BackendName)
	}
	if flushType != flushTypeInsights && apiKey != "" {
		log.Warnf("[%s] api-key is not required", BackendName)
	}
	if flushInterval.Seconds() < 10 {
		log.Warnf("[%s] flushInterval (%s) is recommended to be >= 10s", BackendName, flushInterval)
	} else {
		log.Infof("[%s] flushInterval default (%s)", BackendName, flushInterval)
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
	return &Client{
		address:               address,
		eventType:             eventType,
		flushType:             flushType,
		apiKey:                apiKey,
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

func newInfraPayload(data interface{}) NewRelicInfraPayload {
	return NewRelicInfraPayload{
		Name:               integrationName,
		ProtocolVersion:    protocolVersion,
		IntegrationVersion: integrationVersion,
		Data: []interface{}{
			data,
		},
	}
}

// contains checks if item is within slice
func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
