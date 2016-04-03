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

	"github.com/jtblin/gostatsd/backend"
	"github.com/jtblin/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/cenkalti/backoff"
	"github.com/spf13/viper"
)

const (
	apiURL             = "https://app.datadoghq.com/api/v1/series"
	backendName        = "datadog"
	dogstatsdVersion   = "5.6.3"
	dogstatsdUserAgent = "python-requests/2.6.0 CPython/2.7.10"
	// GAUGE is datadog gauge type.
	GAUGE = "gauge"
	// RATE is datadog rate type.
	RATE = "rate"
)

// Client represents a Datadog client.
type Client struct {
	APIKey      string
	APIEndpoint string
	Hostname    string
	Client      *http.Client
}

const sampleConfig = `
[datadog]
	## Datadog API key
	api_key = "my-secret-key" # required.

	## Connection timeout.
	# timeout = "5s"
`

// TimeSeries represents a time series data structure.
type TimeSeries struct {
	Series    []*Metric `json:"series"`
	Timestamp int64     `json:"-"`
	Hostname  string    `json:"-"`
}

// Metric represents a metric data structure for Datadog.
type Metric struct {
	Host     string   `json:"host,omitempty"`
	Interval float64  `json:"interval,omitempty"`
	Metric   string   `json:"metric"`
	Points   [1]Point `json:"points"`
	Tags     []string `json:"tags,omitempty"`
	Type     string   `json:"type,omitempty"`
}

// Point is a Datadog data point.
type Point [2]float64

// AddMetric adds a metric to the series.
func (ts *TimeSeries) AddMetric(name, stags, metricType string, value float64, interval time.Duration) {
	hostname, tags := types.ExtractSourceFromTags(stags)
	if hostname == "" {
		hostname = ts.Hostname
	}
	metric := &Metric{
		Host:     hostname,
		Interval: interval.Seconds(),
		Metric:   name,
		Points:   [1]Point{{float64(ts.Timestamp), value}},
		Tags:     tags.Normalise(),
		Type:     metricType,
	}
	ts.Series = append(ts.Series, metric)
}

// SendMetrics sends metrics to Datadog.
func (d *Client) SendMetrics(metrics types.MetricMap) error {
	if metrics.NumStats == 0 {
		return nil
	}
	ts := TimeSeries{Timestamp: time.Now().Unix(), Hostname: d.Hostname}

	metrics.Counters.Each(func(key, tagsKey string, counter types.Counter) {
		ts.AddMetric(fmt.Sprintf("%s", key), tagsKey, RATE, counter.PerSecond, counter.Flush)
		ts.AddMetric(fmt.Sprintf("%s.count", key), tagsKey, GAUGE, float64(counter.Value), counter.Flush)
	})

	metrics.Timers.Each(func(key, tagsKey string, timer types.Timer) {
		ts.AddMetric(fmt.Sprintf("%s.lower", key), tagsKey, GAUGE, timer.Min, timer.Flush)
		ts.AddMetric(fmt.Sprintf("%s.upper", key), tagsKey, GAUGE, timer.Max, timer.Flush)
		ts.AddMetric(fmt.Sprintf("%s.count", key), tagsKey, GAUGE, float64(timer.Count), timer.Flush)
		ts.AddMetric(fmt.Sprintf("%s.count_ps", key), tagsKey, RATE, timer.PerSecond, timer.Flush)
		ts.AddMetric(fmt.Sprintf("%s.mean", key), tagsKey, GAUGE, timer.Mean, timer.Flush)
		ts.AddMetric(fmt.Sprintf("%s.median", key), tagsKey, GAUGE, timer.Median, timer.Flush)
		ts.AddMetric(fmt.Sprintf("%s.std", key), tagsKey, GAUGE, timer.StdDev, timer.Flush)
		ts.AddMetric(fmt.Sprintf("%s.sum", key), tagsKey, GAUGE, timer.Sum, timer.Flush)
		ts.AddMetric(fmt.Sprintf("%s.sum_squares", key), tagsKey, GAUGE, timer.SumSquares, timer.Flush)
		for _, pct := range timer.Percentiles {
			ts.AddMetric(fmt.Sprintf("%s.%s", key, pct.String()), tagsKey, GAUGE, pct.Float(), timer.Flush)
		}
	})

	metrics.Gauges.Each(func(key, tagsKey string, gauge types.Gauge) {
		ts.AddMetric(fmt.Sprintf("%s", key), tagsKey, GAUGE, gauge.Value, gauge.Flush)
	})

	metrics.Sets.Each(func(key, tagsKey string, set types.Set) {
		ts.AddMetric(fmt.Sprintf("%s", key), tagsKey, GAUGE, float64(len(set.Values)), set.Flush)
	})

	ts.AddMetric("statsd.numStats", "", GAUGE, float64(metrics.NumStats), metrics.FlushInterval)
	ts.AddMetric("statsd.processingTime", "", GAUGE, float64(metrics.ProcessingTime)/float64(time.Millisecond), metrics.FlushInterval)

	tsBytes, err := json.Marshal(ts)
	if err != nil {
		return fmt.Errorf("[%s] unable to marshal TimeSeries, %s", backendName, err.Error())
	}
	log.Debugf("[%s] json: %s", backendName, tsBytes)
	req, err := http.NewRequest("POST", d.authenticatedURL(), bytes.NewBuffer(tsBytes))
	if err != nil {
		return fmt.Errorf("[%s] unable to create http.Request, %s", backendName, err.Error())
	}
	req.Header.Add("Content-Type", "application/json")
	// Mimic dogstatsd code
	req.Header.Add("DD-Dogstatsd-Version", dogstatsdVersion)
	req.Header.Add("User-Agent", dogstatsdUserAgent)

	post := func(req *http.Request) func() error {
		return func() error {
			resp, e := d.Client.Do(req)
			if e != nil {
				return fmt.Errorf("error POSTing metrics, %s", strings.Replace(err.Error(), d.APIKey, "*****", -1))
			}
			defer resp.Body.Close()

			if resp.StatusCode < 200 || resp.StatusCode > 209 {
				return fmt.Errorf("received bad status code, %d", resp.StatusCode)
			}
			return nil
		}
	}

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 10 * time.Second
	err = backoff.Retry(post(req), b)
	if err != nil {
		return fmt.Errorf("[%s] %s", backendName, err.Error())
	}

	return nil
}

// SampleConfig returns the sample config for the datadog backend.
func (d *Client) SampleConfig() string {
	return sampleConfig
}

// BackendName returns the name of the backend.
func (d *Client) BackendName() string {
	return backendName
}

func (d *Client) authenticatedURL() string {
	q := url.Values{
		"api_key": []string{d.APIKey},
	}
	return fmt.Sprintf("%s?%s", d.APIEndpoint, q.Encode())
}

// NewClient returns a new Datadog API client.
func NewClient(apiKey string, clientTimeout time.Duration) (*Client, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("[%s] api_key is a required field", backendName)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	return &Client{
		APIKey:      apiKey,
		APIEndpoint: apiURL,
		Hostname:    hostname,
		Client: &http.Client{
			Timeout: clientTimeout,
		},
	}, nil
}

func init() {
	backend.RegisterBackend(backendName, func(v *viper.Viper) (backend.MetricSender, error) {
		v.SetDefault("datadog.timeout", time.Duration(5)*time.Second)
		return NewClient(v.GetString("datadog.api_key"), v.GetDuration("datadog.timeout"))
	})
}
