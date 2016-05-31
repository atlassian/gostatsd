package types

import "time"

// Gauge is used for storing aggregated values for gauges.
type Gauge struct {
	Value    float64 // The numeric value of the metric
	Interval         // The flush and expiration interval information
}

// NewGauge initialises a new gauge.
func NewGauge(timestamp time.Time, flushInterval time.Duration, value float64) Gauge {
	return Gauge{Value: value, Interval: Interval{Timestamp: timestamp, Flush: flushInterval}}
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

// Each iterates over each gauge.
func (g Gauges) Each(f func(string, string, Gauge)) {
	for key, value := range g {
		for tags, gauge := range value {
			f(key, tags, gauge)
		}
	}
}
