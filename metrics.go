package gostatsd

import (
	"bytes"
	"fmt"
	"hash/adler32"
	"time"
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
	Tags        Tags       // The tags for the metric
	StringValue string     // The string value for some metrics e.g. Set
	Hostname    string     // Hostname of the source of the metric
	SourceIP    IP         // IP of the source of the metric
	Type        MetricType // The type of metric
	Internal    bool       // If the metric is internally generated
}

// Bucket will pick a distribution bucket for this metric to land in.  max is inclusive.
func (m *Metric) Bucket(max int) int {
	if m.Internal {
		// bucket 0 is reserved for internal metrics
		return 0
	}

	// This gives 0 to max-1, 0 is reserved and max is allowed
	bucket := adler32.Checksum([]byte(m.Name)) % uint32(max)
	return 1 + int(bucket)
}

func (m *Metric) String() string {
	return fmt.Sprintf("{%s, %s, %f, %s, %v}", m.Type, m.Name, m.Value, m.StringValue, m.Tags)
}

// AggregatedMetrics is an interface for aggregated metrics.
type AggregatedMetrics interface {
	MetricsName() string
	Delete(string)
	DeleteChild(string, string)
	HasChildren(string) bool
}

// MetricMap is used for storing aggregated Metric values.
// The keys of each map are metric names.
type MetricMap struct {
	FlushInterval time.Duration
	Counters      Counters
	Timers        Timers
	Gauges        Gauges
	Sets          Sets
}

func (m *MetricMap) String() string {
	buf := new(bytes.Buffer)
	m.Counters.Each(func(k, tags string, counter Counter) {
		fmt.Fprintf(buf, "stats.counter.%s: %d tags=%s\n", k, counter.Value, tags)
	})
	m.Timers.Each(func(k, tags string, timer Timer) {
		for _, value := range timer.Values {
			fmt.Fprintf(buf, "stats.timer.%s: %f tags=%s\n", k, value, tags)
		}
	})
	m.Gauges.Each(func(k, tags string, gauge Gauge) {
		fmt.Fprintf(buf, "stats.gauge.%s: %f tags=%s\n", k, gauge.Value, tags)
	})
	m.Sets.Each(func(k, tags string, set Set) {
		fmt.Fprintf(buf, "stats.set.%s: %d tags=%s\n", k, len(set.Values), tags)
	})
	return buf.String()
}
