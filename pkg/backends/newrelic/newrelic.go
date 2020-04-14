package newrelic

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"
	"github.com/atlassian/gostatsd/pkg/transport"
	"github.com/atlassian/gostatsd/pkg/util"

	"github.com/cenkalti/backoff"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	// BackendName is the name of this backend.
	BackendName                  = "newrelic"
	integrationName              = "com.newrelic.gostatsd"
	integrationVersion           = "2.3.1"
	protocolVersion              = "2"
	defaultUserAgent             = "gostatsd"
	defaultMaxRequestElapsedTime = 15 * time.Second
	// defaultMetricsPerBatch is the default number of metrics to send in a single batch.
	defaultMetricsPerBatch = 1000
	// maxResponseSize is the maximum response size we are willing to read.
	maxResponseSize = 10 * 1024

	flushTypeInsights = "insights"
	flushTypeInfra    = "infra"
	flushTypeMetrics  = "metrics"
)

var (
	// Available flushTypes
	flushTypes = []string{flushTypeInsights, flushTypeInfra, flushTypeMetrics}

	// defaultMaxRequests is the number of parallel outgoing requests to New Relic.  As this mixes both
	// CPU (JSON encoding, TLS) and network bound operations, balancing may require some experimentation.
	defaultMaxRequests = uint(2 * runtime.NumCPU())
)

// Client represents a New Relic client.
type Client struct {
	address        string
	addressMetrics string
	eventType      string
	flushType      string
	apiKey         string
	tagPrefix      string
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
	seriesSent     uint64 // Accumulated number of series successfully sent

	userAgent             string
	maxRequestElapsedTime time.Duration
	client                *http.Client
	metricsPerBatch       uint
	metricsBufferSem      chan *bytes.Buffer // Two in one - a semaphore and a buffer pool
	now                   func() time.Time   // Returns current time. Useful for testing.

	disabledSubtypes gostatsd.TimerSubtypes
	flushInterval    time.Duration
}

// NRInfraPayload represents New Relic Infrastructure Payload format
// https://github.com/newrelic/infra-integrations-sdk/blob/master/docs/v2tov3.md#v2-json-full-sample
type NRInfraPayload struct {
	Name               string        `json:"name"`
	ProtocolVersion    string        `json:"protocol_version"`
	IntegrationVersion string        `json:"integration_version"`
	Data               []interface{} `json:"data"`
}

// NRMetricsPayload represents New Relic Metrics Payload format
// https://docs.newrelic.com/docs/data-ingest-apis/get-data-new-relic/metric-api/report-metrics-metric-api#new-relic-guidelines
type NRMetricsPayload struct {
	Common  NRMetricsCommon `json:"common"`
	Metrics []interface{}   `json:"metrics"`
}

// NRMetricsCommon common attributes to apply for New Relic Metrics Format
type NRMetricsCommon struct {
	Attributes map[string]interface{} `json:"attributes"`
	IntervalMs float64                `json:"interval.ms"`
}

// NRMetric metric for New Relic Metrics Format
type NRMetric struct {
	Name       string                 `json:"name"`
	Value      interface{}            `json:"value,omitempty"`
	Type       string                 `json:"type,omitempty"`
	Timestamp  int64                  `json:"timestamp,omitempty"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	IntervalMs float64                `json:"interval.ms,omitempty"`
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
				if err != nil {
					atomic.AddUint64(&n.seriesSent, uint64(len(ts.Metrics)))
				}

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

// RunMetrics run metrics
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
			statser.Gauge("backend.series.sent", float64(atomic.LoadUint64(&n.seriesSent)), nil)
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
		fl.addMetric(n, "gauge", g.Value, 0, g.Hostname, g.Tags, key)
		fl.maybeFlush()
	})

	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		fl.addMetric(n, "counter", float64(counter.Value), counter.PerSecond, counter.Hostname, counter.Tags, key)
		fl.maybeFlush()
	})

	metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		fl.addMetric(n, "set", float64(len(set.Values)), 0, set.Hostname, set.Tags, key)
		fl.maybeFlush()
	})

	metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		if timer.Histogram != nil {
			for histogramThreshold, count := range timer.Histogram {
				bucketTag := "le:infinity"
				if !math.IsInf(float64(histogramThreshold), 1) {
					bucketTag = "le:" + strconv.FormatFloat(float64(histogramThreshold), 'f', -1, 64)
				}
				newTags := timer.Tags.Concat(gostatsd.Tags{bucketTag})
				fl.addMetric(n, "counter", float64(count), 0, timer.Hostname, newTags, key+".histogram")
			}
		} else {
			fl.addTimerMetric(n, "timer", timer, tagsKey, key)
		}
		fl.maybeFlush()
	})

	fl.finish()
}

// SendEvent sends an event to New Relic.
func (n *Client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	if n.address != "" {
		// Event format depends on flush type
		data := n.EventFormatter(e)
		b, err := json.Marshal(data)
		if err != nil {
			return err
		}

		post, err := n.postWrapper(ctx, b, "events")
		if err != nil {
			return err
		}

		return post()
	}
	return nil
}

// EventFormatter formats gostatsd events
func (n *Client) EventFormatter(e *gostatsd.Event) interface{} {
	event := map[string]interface{}{
		"name":           "event",
		"Title":          e.Title,
		"Text":           e.Text,
		"DateHappened":   e.DateHappened,
		"Hostname":       e.Hostname,
		"AggregationKey": e.AggregationKey,
		"SourceTypeName": e.SourceTypeName,
		"Priority":       e.Priority.StringWithEmptyDefault(),
		"AlertType":      e.AlertType.StringWithEmptyDefault(),
	}
	n.setTags(e.Tags, event)

	switch n.flushType {
	case flushTypeInsights, flushTypeMetrics:
		event["eventType"] = n.eventType
		return []interface{}{event}
	default:
		event["event_type"] = n.eventType
		return newInfraPayload(map[string]interface{}{
			"metrics": []interface{}{event},
		})
	}
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
	case flushTypeMetrics:
		NRPayload := n.newMetricsPayload(data.(*timeSeries).Metrics)
		mJSON, mErr = json.Marshal([]interface{}{NRPayload})
	default:
		NRPayload := newInfraPayload(data)
		mJSON, mErr = json.Marshal(NRPayload)
	}

	if mErr != nil {
		return nil, fmt.Errorf("[%s] unable to marshal: %v", BackendName, mErr)
	}

	return n.postWrapper(ctx, mJSON, "metrics")
}

// postWrapper compresses JSON for Insights
func (n *Client) postWrapper(ctx context.Context, json []byte, dataType string) (func() error, error) {
	return func() error {
		headers := map[string]string{
			"Content-Type": "application/json",
			"User-Agent":   n.userAgent,
		}

		// Insights Event API requires gzip or deflate compression
		// https://docs.newrelic.com/docs/insights/insights-data-sources/custom-data/introduction-event-api#h2-basic-workflow
		// Metrics API requires gzip or identity
		// https://docs.newrelic.com/docs/data-ingest-apis/get-data-new-relic/metric-api/report-metrics-metric-api#headers-query-parameters
		// Use GZIP as standard across both
		if (n.flushType == flushTypeInsights || n.flushType == flushTypeMetrics) && n.apiKey != "" {
			headers["X-Insert-Key"] = n.apiKey
			headers["Content-Encoding"] = "gzip"

			// compress json
			var buf bytes.Buffer
			zw := gzip.NewWriter(&buf)
			_, err := zw.Write([]byte(json))
			if err != nil {
				return err
			}

			// Close to ensure a flush
			if err := zw.Close(); err != nil {
				return err
			}
			json = buf.Bytes()
		}

		address := n.address
		if n.flushType == flushTypeMetrics && dataType == "metrics" {
			address = n.addressMetrics
		}

		req, err := http.NewRequest("POST", address, bytes.NewBuffer(json))
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
func NewClientFromViper(v *viper.Viper, pool *transport.TransportPool) (gostatsd.Backend, error) {
	nr := util.GetSubViper(v, "newrelic")
	nr.SetDefault("transport", "default")
	nr.SetDefault("address", "http://localhost:8001/v1/data")
	nr.SetDefault("event-type", "GoStatsD")

	if strings.Contains(nr.GetString("address-metrics"), "metric-api.newrelic.com") {
		nr.SetDefault("flush-type", flushTypeMetrics)
	} else if strings.Contains(nr.GetString("address"), "insights-collector.newrelic.com") {
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
	nr.SetDefault("max-request-elapsed-time", defaultMaxRequestElapsedTime)
	nr.SetDefault("max-requests", defaultMaxRequests)
	nr.SetDefault("user-agent", defaultUserAgent)

	// New Relic Config Defaults & Recommendations
	v.SetDefault("statser-type", "null")
	v.SetDefault("flush-interval", "10s")
	if v.GetString("statser-type") == "null" {
		log.Infof("[%s] internal metrics OFF, to enable set 'statser-type' to 'logging' or 'internal'", BackendName)
	}

	return NewClient(
		nr.GetString("transport"),
		nr.GetString("address"),
		nr.GetString("address-metrics"),
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
		nr.GetInt("metrics-per-batch"),
		uint(nr.GetInt("max-requests")),
		nr.GetDuration("max-request-elapsed-time"),
		v.GetDuration("flush-interval"), // Main viper, not sub-viper
		gostatsd.DisabledSubMetrics(v),
		pool,
	)
}

// NewClient returns a new New Relic client.
func NewClient(transport, address, addressMetrics, eventType, flushType, apiKey, tagPrefix,
	metricName, metricType, metricPerSecond, metricValue,
	timerMin, timerMax, timerCount, timerMean, timerMedian, timerStdDev, timerSum, timerSumSquares,
	userAgent string, metricsPerBatch int, maxRequests uint,
	maxRequestElapsedTime, flushInterval time.Duration,
	disabled gostatsd.TimerSubtypes, pool *transport.TransportPool) (*Client, error) {

	logger := log.WithField("backend", BackendName)

	if metricsPerBatch <= 0 {
		return nil, fmt.Errorf("[%s] metricsPerBatch must be positive", BackendName)
	}
	if maxRequestElapsedTime <= 0 && maxRequestElapsedTime != -1 {
		return nil, fmt.Errorf("[%s] maxRequestElapsedTime must be positive", BackendName)
	}
	if !contains(flushTypes, flushType) && flushType != "" {
		return nil, fmt.Errorf("[%s] flushType (%s) is not supported", BackendName, flushType)
	}
	if (flushType == flushTypeInsights || flushType == flushTypeMetrics) && apiKey == "" {
		return nil, fmt.Errorf("[%s] api-key is required to flush to insights & metrics backends", BackendName)
	}
	if flushType != flushTypeInsights && flushType != flushTypeMetrics && apiKey != "" {
		logger.Warnf("api-key is not required when not using insights or metrics")
	}
	if flushInterval.Seconds() < 10 {
		logger.Warnf("flushInterval (%s) is recommended to be >= 10s", flushInterval)
	} else {
		logger.Infof("flushInterval default (%s)", flushInterval)
	}

	httpClient, err := pool.Get(transport)
	if err != nil {
		logger.WithError(err).Error("failed to create http client")
		return nil, err
	}
	logger.WithFields(log.Fields{
		"max-request-elapsed-time": maxRequestElapsedTime,
		"max-requests":             maxRequests,
		"metrics-per-batch":        metricsPerBatch,
		"flush-interval":           flushInterval,
	}).Info("created backend")

	metricsBufferSem := make(chan *bytes.Buffer, maxRequests)
	for i := uint(0); i < maxRequests; i++ {
		metricsBufferSem <- &bytes.Buffer{}
	}
	return &Client{
		address:               address,
		addressMetrics:        addressMetrics,
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
		client:                httpClient.Client,
		metricsPerBatch:       uint(metricsPerBatch),
		metricsBufferSem:      metricsBufferSem,
		now:                   time.Now,
		flushInterval:         flushInterval,
		disabledSubtypes:      disabled,
	}, nil
}

func newInfraPayload(data interface{}) NRInfraPayload {
	return NRInfraPayload{
		Name:               integrationName,
		ProtocolVersion:    protocolVersion,
		IntegrationVersion: integrationVersion,
		Data: []interface{}{
			data,
		},
	}
}

func (n *Client) newMetricsPayload(data []interface{}) NRMetricsPayload {
	return NRMetricsPayload{
		Common: NRMetricsCommon{
			Attributes: map[string]interface{}{
				"integration.version": integrationVersion,
				"integration.name":    "GoStatsD",
			},
			IntervalMs: n.flushInterval.Seconds() * 1000,
		},
		Metrics: data,
	}
}

func (n *Client) setTags(tags gostatsd.Tags, data map[string]interface{}) {
	for _, tag := range tags {
		if strings.Contains(tag, ":") {
			keyvalpair := strings.SplitN(tag, ":", 2)
			parsed, err := strconv.ParseFloat(keyvalpair[1], 64)
			if err != nil || strings.EqualFold(keyvalpair[1], "infinity") {
				data[n.tagPrefix+keyvalpair[0]] = keyvalpair[1]
			} else {
				data[n.tagPrefix+keyvalpair[0]] = parsed
			}
		} else {
			data[n.tagPrefix+tag] = "true"
		}
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
