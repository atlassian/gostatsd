package gostatsd

// Counter is used for storing aggregated values for counters.
type Counter struct {
	PerSecond float64  // The calculated per second rate
	Value     int64    // The numeric value of the metric
	Timestamp Nanotime // Last time value was updated
	Source    Source   // Source of the metric
	Tags      Tags     // The tags for the counter
}

// NewCounter initialises a new counter.
func NewCounter(timestamp Nanotime, value int64, source Source, tags Tags) Counter {
	return Counter{Value: value, Timestamp: timestamp, Source: source, Tags: tags.Copy()}
}

func (c *Counter) AddTagsSetSource(additionalTags Tags, newSource Source) {
	c.Tags = c.Tags.Concat(additionalTags)
	c.Source = newSource
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
func (c Counters) Each(f func(metricName string, tagsKey string, c Counter)) {
	for key, value := range c {
		for tags, counter := range value {
			f(key, tags, counter)
		}
	}
}
