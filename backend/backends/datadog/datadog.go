package datadog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	backendTypes "github.com/atlassian/gostatsd/backend/types"
	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/cenkalti/backoff"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

const (
	apiURL = "https://app.datadoghq.com"
	// BackendName is the name of this backend.
	BackendName                                = "datadog"
	dogstatsdVersion                           = "5.6.3"
	dogstatsdUserAgent                         = "python-requests/2.6.0 CPython/2.7.10"
	defaultMaxRequestElapsedTime time.Duration = 10 * time.Second
	defaultClientTimeout         time.Duration = 5 * time.Second
	// gauge is datadog gauge type.
	gauge = "gauge"
	// rate is datadog rate type.
	rate = "rate"
)

// client represents a Datadog client.
type client struct {
	apiKey                string
	apiEndpoint           string
	hostname              string
	maxRequestElapsedTime time.Duration
	client                *http.Client
}

const sampleConfig = `
[datadog]
	## Datadog API key
	api_key = "my-secret-key" # required.

	## Connection timeout.
	# timeout = "5s"
`

// timeSeries represents a time series data structure.
type timeSeries struct {
	Series    []metric `json:"series"`
	Timestamp int64    `json:"-"`
	Hostname  string   `json:"-"`
}

// metric represents a metric data structure for Datadog.
type metric struct {
	Host     string   `json:"host,omitempty"`
	Interval float64  `json:"interval,omitempty"`
	Metric   string   `json:"metric"`
	Points   [1]point `json:"points"`
	Tags     []string `json:"tags,omitempty"`
	Type     string   `json:"type,omitempty"`
}

// point is a Datadog data point.
type point [2]float64

// AddMetric adds a metric to the series.
func (ts *timeSeries) addMetric(name, stags, metricType string, value float64, interval time.Duration) {
	hostname, tags := types.ExtractSourceFromTags(stags)
	if hostname == "" {
		hostname = ts.Hostname
	}
	ts.Series = append(ts.Series, metric{
		Host:     hostname,
		Interval: interval.Seconds(),
		Metric:   name,
		Points:   [1]point{{float64(ts.Timestamp), value}},
		Tags:     tags.Normalise(),
		Type:     metricType,
	})
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

// SendMetrics sends metrics to Datadog.
func (d *client) SendMetrics(ctx context.Context, metrics *types.MetricMap) error {
	if metrics.NumStats == 0 {
		return nil
	}
	return d.postMetrics(d.prepareSeries(metrics))
}

// SendMetricsAsync flushes the metrics to Datadog, preparing payload synchronously but doing the send asynchronously.
func (d *client) SendMetricsAsync(ctx context.Context, metrics *types.MetricMap, cb backendTypes.SendCallback) {
	if metrics.NumStats == 0 {
		cb(nil)
		return
	}
	ts := d.prepareSeries(metrics)
	go func() {
		cb(d.postMetrics(ts))
	}()
}

func (d *client) prepareSeries(metrics *types.MetricMap) *timeSeries {
	ts := timeSeries{Timestamp: time.Now().Unix(), Hostname: d.hostname}

	metrics.Counters.Each(func(key, tagsKey string, counter types.Counter) {
		ts.addMetric(key, tagsKey, rate, counter.PerSecond, counter.Flush)
		ts.addMetric(fmt.Sprintf("%s.count", key), tagsKey, gauge, float64(counter.Value), counter.Flush)
	})

	metrics.Timers.Each(func(key, tagsKey string, timer types.Timer) {
		ts.addMetric(fmt.Sprintf("%s.lower", key), tagsKey, gauge, timer.Min, timer.Flush)
		ts.addMetric(fmt.Sprintf("%s.upper", key), tagsKey, gauge, timer.Max, timer.Flush)
		ts.addMetric(fmt.Sprintf("%s.count", key), tagsKey, gauge, float64(timer.Count), timer.Flush)
		ts.addMetric(fmt.Sprintf("%s.count_ps", key), tagsKey, rate, timer.PerSecond, timer.Flush)
		ts.addMetric(fmt.Sprintf("%s.mean", key), tagsKey, gauge, timer.Mean, timer.Flush)
		ts.addMetric(fmt.Sprintf("%s.median", key), tagsKey, gauge, timer.Median, timer.Flush)
		ts.addMetric(fmt.Sprintf("%s.std", key), tagsKey, gauge, timer.StdDev, timer.Flush)
		ts.addMetric(fmt.Sprintf("%s.sum", key), tagsKey, gauge, timer.Sum, timer.Flush)
		ts.addMetric(fmt.Sprintf("%s.sum_squares", key), tagsKey, gauge, timer.SumSquares, timer.Flush)
		for _, pct := range timer.Percentiles {
			ts.addMetric(fmt.Sprintf("%s.%s", key, pct.String()), tagsKey, gauge, pct.Float(), timer.Flush)
		}
	})

	metrics.Gauges.Each(func(key, tagsKey string, g types.Gauge) {
		ts.addMetric(key, tagsKey, gauge, g.Value, g.Flush)
	})

	metrics.Sets.Each(func(key, tagsKey string, set types.Set) {
		ts.addMetric(key, tagsKey, gauge, float64(len(set.Values)), set.Flush)
	})
	return &ts
}

func (d *client) postMetrics(ts *timeSeries) error {
	return d.post("/api/v1/series", "metrics", ts)
}

// SendEvent sends an event to Datadog.
func (d *client) SendEvent(ctx context.Context, e *types.Event) error {
	return d.post("/api/v1/events", "events", event{
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

// SampleConfig returns the sample config for the datadog backend.
func (d *client) SampleConfig() string {
	return sampleConfig
}

// BackendName returns the name of the backend.
func (d *client) BackendName() string {
	return BackendName
}

func (d *client) post(path, typeOfPost string, data interface{}) error {
	tsBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("[%s] unable to marshal %s: %v", BackendName, typeOfPost, err)
	}
	log.Debugf("[%s] %s json: %s", BackendName, typeOfPost, tsBytes)

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = d.maxRequestElapsedTime
	err = backoff.RetryNotify(d.doPost(path, tsBytes), b, func(err error, d time.Duration) {
		log.Warnf("[%s] failed to send %s, sleeping for %s: %v", BackendName, typeOfPost, d, err)
	})
	if err != nil {
		return fmt.Errorf("[%s] %v", BackendName, err)
	}

	return nil
}

func (d *client) doPost(path string, body []byte) backoff.Operation {
	authenticatedURL := d.authenticatedURL(path)
	return func() error {
		req, err := http.NewRequest("POST", authenticatedURL, bytes.NewBuffer(body))
		if err != nil {
			return fmt.Errorf("unable to create http.Request: %v", err)
		}
		req.Header.Add("Content-Type", "application/json")
		// Mimic dogstatsd code
		req.Header.Add("DD-Dogstatsd-Version", dogstatsdVersion)
		req.Header.Add("User-Agent", dogstatsdUserAgent)
		resp, err := d.client.Do(req)
		if err != nil {
			return fmt.Errorf("error POSTing: %s", strings.Replace(err.Error(), d.apiKey, "*****", -1))
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 205 {
			return fmt.Errorf("received bad status code %d", resp.StatusCode)
		}
		return nil
	}
}

func (d *client) authenticatedURL(path string) string {
	q := url.Values{
		"api_key": []string{d.apiKey},
	}
	return fmt.Sprintf("%s%s?%s", d.apiEndpoint, path, q.Encode())
}

// NewClientFromViper returns a new Datadog API client.
func NewClientFromViper(v *viper.Viper) (backendTypes.Backend, error) {
	dd := getSubViper(v, "datadog")
	dd.SetDefault("api_endpoint", apiURL)
	dd.SetDefault("timeout", defaultClientTimeout)
	dd.SetDefault("max_request_elapsed_time", defaultMaxRequestElapsedTime)
	return NewClient(
		dd.GetString("api_endpoint"),
		dd.GetString("api_key"),
		dd.GetDuration("timeout"),
		dd.GetDuration("max_request_elapsed_time"),
	)
}

// NewClient returns a new Datadog API client.
func NewClient(apiEndpoint, apiKey string, clientTimeout, maxRequestElapsedTime time.Duration) (backendTypes.Backend, error) {
	if apiEndpoint == "" {
		return nil, fmt.Errorf("[%s] apiEndpoint is required", BackendName)
	}
	if apiKey == "" {
		return nil, fmt.Errorf("[%s] apiKey is required", BackendName)
	}
	if clientTimeout <= 0 {
		return nil, fmt.Errorf("[%s] clientTimeout must be positive", BackendName)
	}
	if maxRequestElapsedTime <= 0 {
		return nil, fmt.Errorf("[%s] maxRequestElapsedTime must be positive", BackendName)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("[%s] cannot get hostname: %v", BackendName, err)
	}
	log.Infof("[%s] maxRequestElapsedTime=%s clientTimeout=%s", BackendName, maxRequestElapsedTime, clientTimeout)
	return &client{
		apiKey:                apiKey,
		apiEndpoint:           apiEndpoint,
		hostname:              hostname,
		maxRequestElapsedTime: maxRequestElapsedTime,
		client: &http.Client{
			Timeout: clientTimeout,
		},
	}, nil
}

// Workaround https://github.com/spf13/viper/pull/165 and https://github.com/spf13/viper/issues/191
func getSubViper(v *viper.Viper, key string) *viper.Viper {
	var n *viper.Viper
	namespace := v.Get(key)
	if namespace != nil {
		n = v.Sub(key)
	}
	if n == nil {
		n = viper.New()
	}
	return n
}
