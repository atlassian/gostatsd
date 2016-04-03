package types

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// MetricType is an enumeration of all the possible types of Metric.
type MetricType float64

// StatsdSourceID stores the key used to tag metrics with the origin IP address.
const StatsdSourceID = "statsd_source_id"

const (
	_ = iota
	// COUNTER is statsd counter type
	COUNTER MetricType = 1 << (10 * iota)
	// TIMER is statsd timer type
	TIMER
	// GAUGE is statsd gauge type
	GAUGE
	// SET is statsd set type
	SET
)

// Regular expressions used for metric name normalization.
var (
	regDot       = regexp.MustCompile("\\.")
	regSemiColon = regexp.MustCompile(":")
)

// TagToMetricName transforms tags into metric names.
func TagToMetricName(tag string) string {
	return regSemiColon.ReplaceAllString(tag, ".")
}

// NormalizeTagElement cleans up the key or the value of a tag.
func NormalizeTagElement(name string) string {
	element := regSemiColon.ReplaceAllString(name, "_")
	element = regDot.ReplaceAllString(element, "_")
	return strings.ToLower(element)
}

func (m MetricType) String() string {
	switch {
	case m >= SET:
		return "set"
	case m >= GAUGE:
		return "gauge"
	case m >= TIMER:
		return "timer"
	case m >= COUNTER:
		return "counter"
	}
	return "unknown"
}

// Metric represents a single data collected datapoint.
type Metric struct {
	Type        MetricType // The type of metric
	Name        string     // The name of the metric
	Value       float64    // The numeric value of the metric
	Tags        Tags       // The tags for the metric
	StringValue string     // The string value for some metrics e.g. Set
}

// NewMetric creates a metric with tags.
func NewMetric(name string, value float64, mtype MetricType, tags Tags) *Metric {
	return &Metric{
		Type:  mtype,
		Name:  name,
		Tags:  tags,
		Value: value,
	}
}

// Tags represents a list of tags.
type Tags []string

// String sorts the tags alphabetically and returns
// a comma-separated string representation of the tags.
func (tags Tags) String() string {
	sort.Strings(tags)
	return strings.Join(tags, ",")
}

// Map returns a map of the tags.
func (tags Tags) Map() map[string]string {
	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		s := strings.Split(tag, ":")
		tagMap[s[0]] = ""
		if len(s) > 1 {
			tagMap[s[0]] = s[1]
		}
	}
	return tagMap

}

// ExtractSourceFromTags returns the source from the tags
// and the updated tags.
func ExtractSourceFromTags(s string) (string, Tags) {
	tags := Tags(strings.Split(s, ","))
	idx, element := tags.IndexOfKey(StatsdSourceID)
	if idx != -1 {
		bits := strings.Split(element, ":")
		if len(bits) > 1 {
			return bits[1], append(tags[:idx], tags[idx+1:]...)
		}
	}
	return "", tags
}

// IndexOfKey returns the index and the element starting with the string key.
func (tags Tags) IndexOfKey(key string) (int, string) {
	for i, v := range tags {
		if strings.HasPrefix(v, key+":") {
			return i, v
		}
	}
	return -1, ""
}

// Normalise normalises tags as key:value.
func (tags Tags) Normalise() Tags {
	nTags := Tags{}
	for _, tag := range tags {
		if tag != "" {
			if !strings.Contains(tag, ":") {
				tag = "tag:" + tag
			}
			nTags = append(nTags, tag)
		}
	}
	return nTags
}

func (m Metric) String() string {
	return fmt.Sprintf("{%s, %s, %f, %s, %v}", m.Type, m.Name, m.Value, m.StringValue, m.Tags)
}

// AggregatedMetrics is an interface for aggregated metrics.
type AggregatedMetrics interface {
	MetricsName() string
	Delete(string)
	DeleteChild(string, string)
	HasChildren(string) bool
}

// Counters stores a map of counters by tags.
type Counters map[string]map[string]Counter

// MetricsName returns the name of the aggregated metrics collection.
func (c Counters) MetricsName() string {
	return "Counters"
}

// Delete deletes the metrics from the collection.
func (c Counters) Delete(k string) {
	delete(c, k)
}

// DeleteChild deletes the metrics from the collection for the given tags.
func (c Counters) DeleteChild(k, t string) {
	delete(c[k], t)
}

// HasChildren returns whether there are more children nested under the key.
func (c Counters) HasChildren(k string) bool {
	return len(c[k]) != 0
}

// Timers stores a map of timers by tags.
type Timers map[string]map[string]Timer

// MetricsName returns the name of the aggregated metrics collection.
func (t Timers) MetricsName() string {
	return "Timers"
}

// Delete deletes the metrics from the collection.
func (t Timers) Delete(k string) {
	delete(t, k)
}

// DeleteChild deletes the metrics from the collection for the given tags.
func (t Timers) DeleteChild(k, tags string) {
	delete(t[k], tags)
}

// HasChildren returns whether there are more children nested under the key.
func (t Timers) HasChildren(k string) bool {
	return len(t[k]) != 0
}

// Gauges stores a map of gauges by tags.
type Gauges map[string]map[string]Gauge

// MetricsName returns the name of the aggregated metrics collection.
func (g Gauges) MetricsName() string {
	return "Gauges"
}

// Delete deletes the metrics from the collection.
func (g Gauges) Delete(k string) {
	delete(g, k)
}

// DeleteChild deletes the metrics from the collection for the given tags.
func (g Gauges) DeleteChild(k, t string) {
	delete(g[k], t)
}

// HasChildren returns whether there are more children nested under the key.
func (g Gauges) HasChildren(k string) bool {
	return len(g[k]) != 0
}

// Sets stores a map of sets by tags.
type Sets map[string]map[string]Set

// MetricsName returns the name of the aggregated metrics collection.
func (s Sets) MetricsName() string {
	return "Sets"
}

// Delete deletes the metrics from the collection.
func (s Sets) Delete(k string) {
	delete(s, k)
}

// DeleteChild deletes the metrics from the collection for the given tags.
func (s Sets) DeleteChild(k, t string) {
	delete(s[k], t)
}

// HasChildren returns whether there are more children nested under the key.
func (s Sets) HasChildren(k string) bool {
	return len(s[k]) != 0
}

// MetricMap is used for storing aggregated Metric values.
// The keys of each map are metric names.
type MetricMap struct {
	NumStats       int
	ProcessingTime time.Duration
	FlushInterval  time.Duration
	Counters       Counters
	Timers         Timers
	Gauges         Gauges
	Sets           Sets
}

func (m MetricMap) String() string {
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

// Interval stores the flush interval and timestamp for expiration interval.
type Interval struct {
	Timestamp time.Time
	Flush     time.Duration
}

// Counter is used for storing aggregated values for counters.
type Counter struct {
	PerSecond float64 // The calculated per second rate
	Value     int64   // The numeric value of the metric
	Interval          // The flush and expiration interval information
}

// NewCounter initialises a new counter.
func NewCounter(timestamp time.Time, flushInterval time.Duration, value int64) Counter {
	return Counter{Value: value, Interval: Interval{Timestamp: timestamp, Flush: flushInterval}}
}

// Each iterates over each counter.
func (c Counters) Each(f func(string, string, Counter)) {
	for key, value := range c {
		for tags, counter := range value {
			f(key, tags, counter)
		}
	}
}

// Clone performs a deep copy of a map of counters into a new map.
func (c Counters) Clone() Counters {
	destination := Counters{}
	c.Each(func(key, tags string, counter Counter) {
		if _, ok := destination[key]; !ok {
			destination[key] = make(map[string]Counter)
		}
		destination[key][tags] = counter
	})
	return destination
}

// Percentiles represents an array of percentiles.
type Percentiles []*Percentile

// Percentile is used to store the aggregation for a percentile.
type Percentile struct {
	float float64
	str   string
}

// Set append a percentile aggregation to the percentiles.
func (p *Percentiles) Set(s string, f float64) {
	*p = append(*p, &Percentile{f, strings.Replace(s, ".", "_", -1)})
}

// String returns the string value of percentiles.
func (p *Percentiles) String() string {
	buf := new(bytes.Buffer)
	for _, pct := range *p {
		fmt.Fprintf(buf, "%s:%f ", pct.String(), pct.Float())
	}
	return buf.String()
}

// String returns the string value of a percentile.
func (p *Percentile) String() string {
	return p.str
}

// Float returns the float value of a percentile.
func (p *Percentile) Float() float64 {
	return p.float
}

// Timer is used for storing aggregated values for timers.
type Timer struct {
	Count       int         // The number of timers in the series
	PerSecond   float64     // The calculated per second rate
	Mean        float64     // The mean time of the series
	Median      float64     // The median time of the series
	Min         float64     // The minimum time of the series
	Max         float64     // The maximum time of the series
	StdDev      float64     // The standard deviation for the series
	Sum         float64     // The sum for the series
	SumSquares  float64     // The sum squares for the series
	Values      []float64   // The numeric value of the metric
	Percentiles Percentiles // The percentile aggregations of the metric
	Interval                // The flush and expiration interval information
}

// NewTimer initialises a new timer.
func NewTimer(timestamp time.Time, flushInterval time.Duration, values []float64) Timer {
	return Timer{Values: values, Interval: Interval{Timestamp: timestamp, Flush: flushInterval}}
}

// Each iterates over each timer.
func (t Timers) Each(f func(string, string, Timer)) {
	for key, value := range t {
		for tags, timer := range value {
			f(key, tags, timer)
		}
	}
}

// Clone performs a deep copy of a map of timers into a new map.
func (t Timers) Clone() Timers {
	destination := Timers{}
	t.Each(func(key, tags string, timer Timer) {
		if _, ok := destination[key]; !ok {
			destination[key] = make(map[string]Timer)
		}
		destination[key][tags] = timer
	})
	return destination
}

// Gauge is used for storing aggregated values for gauges.
type Gauge struct {
	Value    float64 // The numeric value of the metric
	Interval         // The flush and expiration interval information
}

// NewGauge initialises a new gauge.
func NewGauge(timestamp time.Time, flushInterval time.Duration, value float64) Gauge {
	return Gauge{Value: value, Interval: Interval{Timestamp: timestamp, Flush: flushInterval}}
}

// Each iterates over each gauge.
func (g Gauges) Each(f func(string, string, Gauge)) {
	for key, value := range g {
		for tags, gauge := range value {
			f(key, tags, gauge)
		}
	}
}

// Clone performs a deep copy of a map of gauges into a new map.
func (g Gauges) Clone() Gauges {
	destination := Gauges{}
	g.Each(func(key, tags string, gauge Gauge) {
		if _, ok := destination[key]; !ok {
			destination[key] = make(map[string]Gauge)
		}
		destination[key][tags] = gauge
	})
	return destination
}

// Set is used for storing aggregated values for sets.
type Set struct {
	Values   map[string]int64 // The number of occurrences for a specific value
	Interval                  // The flush and expiration interval information
}

// NewSet initialises a new set.
func NewSet(timestamp time.Time, flushInterval time.Duration, values map[string]int64) Set {
	return Set{Values: values, Interval: Interval{Timestamp: timestamp, Flush: flushInterval}}
}

// Each iterates over each set.
func (s Sets) Each(f func(string, string, Set)) {
	for key, value := range s {
		for tags, set := range value {
			f(key, tags, set)
		}
	}
}

// Clone performs a deep copy of a map of sets into a new map.
func (s Sets) Clone() Sets {
	destination := Sets{}
	s.Each(func(key, tags string, set Set) {
		if _, ok := destination[key]; !ok {
			destination[key] = make(map[string]Set)
		}
		destination[key][tags] = set
	})
	return destination
}
