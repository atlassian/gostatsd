package types

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

// MetricType is an enumeration of all the possible types of Metric
type MetricType float64

const (
	_                = iota
	ERROR MetricType = 1 << (10 * iota)
	COUNTER
	TIMER
	GAUGE
)

func (m MetricType) String() string {
	switch {
	case m >= GAUGE:
		return "gauge"
	case m >= TIMER:
		return "timer"
	case m >= COUNTER:
		return "counter"
	}
	return "unknown"
}

// Metric represents a single data collected datapoint
type Metric struct {
	Type      MetricType // The type of metric
	Name      string     // The name of the metric
	Value     float64    // The numeric value of the metric
	Tags      Tags       // The tags for the metric
}

// Tags represents a list of tags
type Tags struct {
	Items []string
}

// String sorts the tags alphabetically and returns
// a comma-separated string representation of the tags
func (tags Tags) String() string {
	sort.Strings(tags.Items)
	return strings.Join(tags.Items, ",")
}

// Map returns a map of the tags
func (tags Tags) Map() map[string]string {
	tagMap := make(map[string]string, len(tags.Items))
	for _, tag := range tags.Items {
		s := strings.Split(tag, ":")
		tagMap[s[0]] = ""
		if len(s) > 1 {
			tagMap[s[0]] = s[1]
		}
	}
	return tagMap

}

func (m Metric) String() string {
	return fmt.Sprintf("{%s, %s, %f, %v}", m.Type, m.Name, m.Value, m.Tags)
}

// MetricMap is used for storing aggregated Metric values.
// The keys of each map are metric names.
type MetricMap struct {
	NumStats int
	Counters map[string]map[string]Counter
	Timers   map[string]map[string]Timer
	Gauges   map[string]map[string]Gauge
}

func (m MetricMap) String() string {
	buf := new(bytes.Buffer)
	EachCounter(m.Counters, func(k, tags string, counter Counter) {
		fmt.Fprintf(buf, "stats.counter.%s: %f tags=%s\n", k, counter.Value, tags)
	})
	EachTimer(m.Timers, func(k, tags string, timer Timer) {
		for _, value := range timer.Values {
			fmt.Fprintf(buf, "stats.timer.%s: %f tags=%s\n", k, value, tags)
		}
	})
	EachGauge(m.Gauges, func(k, tags string, gauge Gauge) {
		fmt.Fprintf(buf, "stats.gauge.%s: %f tags=%s\n", k, gauge.Value, tags)
	})
	return buf.String()
}

// Counter is used for storing aggregated values for counters.
type Counter struct {
	PerSecond float64 // The calculated per second rate
	Value     int64   // The numeric value of the metric
}

// EachCounter iterates over each counter
func EachCounter(c map[string]map[string]Counter, f func(string, string, Counter)) {
	for key, value := range c {
		for tags, counter := range value {
			f(key, tags, counter)
		}
	}
}

// Timer is used for storing aggregated values for timers.
type Timer struct {
	Count  int       // The number of timers in the series
	Min    float64   // The minimum time of the series
	Max    float64   // The maximum time of the series
	Values []float64 // The numeric value of the metric
}

// EachTimer iterates over each timer
func EachTimer(c map[string]map[string]Timer, f func(string, string, Timer)) {
	for key, value := range c {
		for tags, timer := range value {
			f(key, tags, timer)
		}
	}
}

// Timer is used for storing aggregated values for gauges.
type Gauge struct {
	Value float64 // The numeric value of the metric
}

// EachGauge iterates over each gauge
func EachGauge(c map[string]map[string]Gauge, f func(string, string, Gauge)) {
	for key, value := range c {
		for tags, gauge := range value {
			f(key, tags, gauge)
		}
	}
}
