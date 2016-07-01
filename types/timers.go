package types

import "time"

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
	Hostname    string      // Hostname of the source of the metric
	Tags        Tags        // The tags for the timer
}

// NewTimer initialises a new timer.
func NewTimer(timestamp time.Time, flushInterval time.Duration, values []float64, hostname string, tags Tags) Timer {
	return Timer{Values: values, Interval: Interval{Timestamp: timestamp, Flush: flushInterval}, Hostname: hostname, Tags: tags}
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

// Each iterates over each timer.
func (t Timers) Each(f func(string, string, Timer)) {
	for key, value := range t {
		for tags, timer := range value {
			f(key, tags, timer)
		}
	}
}
