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
	Name        string     // The name of the metric
	Value       float64    // The numeric value of the metric
	Rate        float64    // The sampling rate of the metric
	Tags        Tags       // The tags for the metric
	TagsKey     string     // The tags rendered as a string to uniquely identify the tagset in a map.  Sort of a cache.  Will be removed at some point.
	StringValue string     // The string value for some metrics e.g. Set
	Hostname    string     // Hostname of the source of the metric
	SourceIP    Source     // IP of the source of the metric
	Timestamp   Nanotime   // Most accurate known timestamp of this metric
	Type        MetricType // The type of metric
	DoneFunc    func()     // Returns the metric to the pool. May be nil. Call Metric.Done(), not this.
}

// Reset is used to reset a metric to as clean state, called on re-use from the pool.
func (m *Metric) Reset() {
	m.Name = ""
	m.Value = 0
	m.Rate = 1
	m.Tags = m.Tags[:0]
	m.TagsKey = ""
	m.StringValue = ""
	m.Hostname = ""
	m.SourceIP = ""
	m.Timestamp = 0
	m.Type = 0
}

// Bucket will pick a distribution bucket for this metric to land in.  max is exclusive.
func (m *Metric) Bucket(max int) int {
	return Bucket(m.Name, m.Hostname, max)
}

func Bucket(metricName, hostname string, max int) int {
	// Consider hashing the tags here too
	bucket := adler32.Checksum([]byte(metricName))
	bucket += adler32.Checksum([]byte(hostname))
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
		m.TagsKey = FormatTagsKey(m.Hostname, m.Tags)
	}
	return m.TagsKey
}

func FormatTagsKey(hostname string, tags Tags) string {
	t := tags.SortedString()
	if hostname == "" {
		return t
	}
	return t + "," + StatsdSourceID + ":" + hostname
}

// AggregatedMetrics is an interface for aggregated metrics.
type AggregatedMetrics interface {
	MetricsName() string
	Delete(string)
	DeleteChild(string, string)
	HasChildren(string) bool
}
