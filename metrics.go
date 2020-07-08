package gostatsd

import (
	"fmt"
	"hash/adler32"
)

// MetricType is an enumeration of all the possible types of Metric.
type MetricType byte

const (
	_ = iota
	// COUNTER is statsd counter type
	COUNTER MetricType = iota
	// TIMER is statsd timer type
	TIMER
	// GAUGE is statsd gauge type
	GAUGE
	// SET is statsd set type
	SET
)

func (m MetricType) String() string {
	switch m {
	case SET:
		return "set"
	case GAUGE:
		return "gauge"
	case TIMER:
		return "timer"
	case COUNTER:
		return "counter"
	}
	return "unknown"
}

// Metric represents a single data collected datapoint.
type Metric struct {
	Name        string  // The name of the metric
	Value       float64 // The numeric value of the metric
	Rate        float64 // The sampling rate of the metric
	Tags        Tags    // The tags for the metric
	TagsKey     string  // The tags rendered as a string to uniquely identify the tagset in a map.  Sort of a cache.  Will be removed at some point.
	StringValue string  // The string value for some metrics e.g. Set
	// Source is the source of the metric, its lifecycle is:
	// - If ignore-host is set, it will be set to the `host` tag if present, otherwise blank.  If ignore-host is not set, it will be set to the sending IP
	// - If the cloud provider is enabled, it will attempt to perform a lookup of this value to find a new value (instance ID, pod ID, etc)
	// - If the tag handler matches a `drop-host` filter, it will be removed
	// - Backends treat it inconsistently
	Source    Source     // Source of the metric.  In order of
	Timestamp Nanotime   // Most accurate known timestamp of this metric
	Type      MetricType // The type of metric
	DoneFunc  func()     // Returns the metric to the pool. May be nil. Call Metric.Done(), not this.
}

// Reset is used to reset a metric to as clean state, called on re-use from the pool.
func (m *Metric) Reset() {
	m.Name = ""
	m.Value = 0
	m.Rate = 1
	m.Tags = m.Tags[:0]
	m.TagsKey = ""
	m.StringValue = ""
	m.Source = ""
	m.Timestamp = 0
	m.Type = 0
}

// Bucket will pick a distribution bucket for this metric to land in.  max is exclusive.
func (m *Metric) Bucket(max int) int {
	return Bucket(m.Name, m.Source, max)
}

func Bucket(metricName string, source Source, max int) int {
	// Consider hashing the tags here too
	bucket := adler32.Checksum([]byte(metricName))
	bucket += adler32.Checksum([]byte(source))
	return int(bucket % uint32(max))
}

func (m *Metric) String() string {
	return fmt.Sprintf("{%s, %s, %f, %s, %v}", m.Type, m.Name, m.Value, m.StringValue, m.Tags)
}

// Done invokes DoneFunc if it's set, returning the metric to the pool.
func (m *Metric) Done() {
	if m.DoneFunc != nil {
		m.DoneFunc()
	}
}

func (m *Metric) FormatTagsKey() string {
	if m.TagsKey == "" {
		m.TagsKey = FormatTagsKey(m.Source, m.Tags)
	}
	return m.TagsKey
}

func FormatTagsKey(source Source, tags Tags) string {
	t := tags.SortedString()
	if source == "" {
		return t
	}
	return t + "," + StatsdSourceID + ":" + string(source)
}

// AggregatedMetrics is an interface for aggregated metrics.
type AggregatedMetrics interface {
	MetricsName() string
	Delete(string)
	DeleteChild(string, string)
	HasChildren(string) bool
}
