package gostatsd

// Set is used for storing aggregated values for sets.
type Set struct {
	Values    map[string]struct{}
	Timestamp Nanotime // Last time value was updated
	Source    Source   // Hostname of the source of the metric
	Tags      Tags     // The tags for the set
}

// NewSet initialises a new set.
func NewSet(timestamp Nanotime, values map[string]struct{}, source Source, tags Tags) Set {
	return Set{Values: values, Timestamp: timestamp, Source: source, Tags: tags.Copy()}
}

func (s *Set) AddTagsSetSource(additionalTags Tags, newSource Source) {
	s.Tags = s.Tags.Concat(additionalTags)
	s.Source = newSource
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
func (s Sets) Each(f func(metricName string, tagsKey string, s Set)) {
	for key, value := range s {
		for tags, set := range value {
			f(key, tags, set)
		}
	}
}
