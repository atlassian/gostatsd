package types

import "time"

// Set is used for storing aggregated values for sets.
type Set struct {
	Values   map[string]struct{}
	Interval // The flush and expiration interval information
}

// NewSet initialises a new set.
func NewSet(timestamp time.Time, flushInterval time.Duration, values map[string]struct{}) Set {
	return Set{Values: values, Interval: Interval{Timestamp: timestamp, Flush: flushInterval}}
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

// Each iterates over each set.
func (s Sets) Each(f func(string, string, Set)) {
	for key, value := range s {
		for tags, set := range value {
			f(key, tags, set)
		}
	}
}
