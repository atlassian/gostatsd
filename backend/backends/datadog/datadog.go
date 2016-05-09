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
)

const (
	apiURL = "https://app.datadoghq.com"
	// BackendName is the name of this backend.
	BackendName        = "datadog"
	dogstatsdVersion   = "5.6.3"
	dogstatsdUserAgent = "python-requests/2.6.0 CPython/2.7.10"
	// gauge is datadog gauge type.
	gauge = "gauge"
	// rate is datadog rate type.
	rate = "rate"
)

// client represents a Datadog client.
type client struct {
	apiKey      string
	apiEndpoint string
	hostname    string
	client      *http.Client
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
	metric := metric{
		Host:     hostname,
		Interval: interval.Seconds(),
		Metric:   name,
		Points:   [1]point{{float64(ts.Timestamp), value}},
		Tags:     tags.Normalise(),
		Type:     metricType,
	}
	ts.Series = append(ts.Series, metric)
}

// SendMetrics sends metrics to Datadog.
func (d *client) SendMetrics(metrics types.MetricMap) error {
	if metrics.NumStats == 0 {
		return nil
	}
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

	tsBytes, err := json.Marshal(ts)
	if err != nil {
		return fmt.Errorf("[%s] unable to marshal TimeSeries: %v", BackendName, err)
	}
	log.Debugf("[%s] json: %s", BackendName, tsBytes)
	req, err := http.NewRequest("POST", d.authenticatedURL("/api/v1/series"), bytes.NewBuffer(tsBytes))
	if err != nil {
		return fmt.Errorf("[%s] unable to create http.Request: %v", BackendName, err)
	}

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 10 * time.Second
	err = backoff.Retry(d.post(req), b)
	if err != nil {
		return fmt.Errorf("[%s] %v", BackendName, err)
	}

	return nil
}

// SampleConfig returns the sample config for the datadog backend.
func (d *client) SampleConfig() string {
	return sampleConfig
}

// BackendName returns the name of the backend.
func (d *client) BackendName() string {
	return BackendName
}

func (d *client) post(req *http.Request) func() error {
	req.Header.Add("Content-Type", "application/json")
	// Mimic dogstatsd code
	req.Header.Add("DD-Dogstatsd-Version", dogstatsdVersion)
	req.Header.Add("User-Agent", dogstatsdUserAgent)
	return func() error {
		resp, err := d.client.Do(req)
		if err != nil {
			return fmt.Errorf("error POSTing metrics: %s", strings.Replace(err.Error(), d.apiKey, "*****", -1))
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
func NewClientFromViper(v *viper.Viper) (backendTypes.MetricSender, error) {
	v.SetDefault("datadog.timeout", time.Duration(5)*time.Second)
	return NewClient(v.GetString("datadog.api_key"), v.GetDuration("datadog.timeout"))
}

// NewClient returns a new Datadog API client.
func NewClient(apiKey string, clientTimeout time.Duration) (backendTypes.MetricSender, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("[%s] api_key is a required field", BackendName)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	return &client{
		apiKey:      apiKey,
		apiEndpoint: apiURL,
		hostname:    hostname,
		client: &http.Client{
			Timeout: clientTimeout,
		},
	}, nil
}
