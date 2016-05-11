package types

import "time"

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
