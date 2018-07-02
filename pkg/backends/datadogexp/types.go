package datadogexp

import (
	"time"

	"github.com/atlassian/gostatsd"
)

// ddMetric represents a metric timeseries for Datadog.
type ddMetric struct {
	Host     string     `json:"host,omitempty"`
	Interval float64    `json:"interval,omitempty"`
	Metric   string     `json:"metric"`
	Points   []ddPoint  `json:"points"`
	Tags     []string   `json:"tags,omitempty"`
	Type     metricType `json:"type,omitempty"`
}

// ddPoint is a tuple consisting of a timestamp and a value
type ddPoint [2]float64

// ddTimeSeries represents a time series data structure.
type ddTimeSeries struct {
	Series []*ddMetric `json:"series"`
}

// ddMetricMap is a map[metricName][tagKey] which holds a string
// indexable list of ddMetrics that are pending flush to Datadog
type ddMetricMap map[string]map[string]*ddMetric

type metricType string

const (
	// gauge is datadog gauge type.
	gauge metricType = "gauge"
	// rate is datadog rate type.
	rate metricType = "rate"
)

type batchMessage struct {
	metrics   *gostatsd.MetricMap
	flushTime time.Time
	done      gostatsd.SendCallback
}

// ddEvent represents an event data structure for Datadog.
type ddEvent struct {
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
