package statsd

import (
	"context"

	"github.com/atlassian/gostatsd"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type TagHandler struct {
	metrics       MetricHandler
	events        EventHandler
	tags          gostatsd.Tags // Tags to add to all metrics
	filters       []Filter
	estimatedTags int
}

func NewTagHandlerFromViper(v *viper.Viper, metrics MetricHandler, events EventHandler, tags gostatsd.Tags) *TagHandler {
	filterNameList := v.GetStringSlice("filters")
	var filters []Filter
	for _, filterName := range filterNameList {
		vFilter := v.Sub("filter." + filterName)
		if vFilter == nil {
			logrus.Warnf("Filter doesn't exist: %v", filterName)
			continue
		}
		filters = append(filters, NewFilterFromViper(filterName, vFilter))
		logrus.Infof("Loaded filter %v", filterName)
	}
	return NewTagHandler(metrics, events, tags, filters)
}

// NewTagHandler initialises a new handler which adds unique tags and sends metrics/events to the next handler
func NewTagHandler(metrics MetricHandler, events EventHandler, tags gostatsd.Tags, filters []Filter) *TagHandler {
	return &TagHandler{
		metrics:       metrics,
		events:        events,
		tags:          tags,
		filters:       filters,
		estimatedTags: len(tags) + metrics.EstimatedTags(),
	}
}

// EstimatedTags returns a guess for how many tags to pre-allocate
func (th *TagHandler) EstimatedTags() int {
	return th.estimatedTags
}

// DispatchMetric adds the unique tags from the TagHandler to the metric and passes it to the next stage in the pipeline
func (th *TagHandler) DispatchMetric(ctx context.Context, m *gostatsd.Metric) error {
	if m.Hostname == "" {
		m.Hostname = string(m.SourceIP)
	}
	m.Tags = uniqueTags(m.Tags, th.tags)
	if th.filterMetricAndTags(m) {
		return th.metrics.DispatchMetric(ctx, m)
	}
	return nil
}

// returns true if the metric should be submitted to the next stage.  Updates the metric tags if needed.
func (th *TagHandler) filterMetricAndTags(m *gostatsd.Metric) bool {
	for _, filter := range th.filters {
		if len(filter.MatchMetrics) > 0 && !filter.MatchMetrics.MatchAny(m.Name) { // returns false if nothing present
			// name doesn't match an include, stop
			continue
		}

		// this list may be empty, and therefore return false
		if filter.ExcludeMetrics.MatchAny(m.Name) { // returns false if nothing present
			// name matches an exclude, stop
			continue
		}

		if len(filter.MatchTags) > 0 && !filter.MatchTags.MatchAnyMultiple(m.Tags) { // returns false if either list is empty
			// no tags match
			continue
		}

		if filter.DropMetric {
			return false
		}

		// TODO: This is not the most efficient.  It might be better to replace with an empty string, and ensure empty
		// tags are handled downstream.
		for _, dropFilter := range filter.DropTags {
			tags := m.Tags[:0]
			for _, tag := range m.Tags {
				if !dropFilter.Match(tag) {
					tags = append(tags, tag)
				}
			}
			m.Tags = tags
		}

		if filter.DropHost {
			m.Hostname = ""
		}
	}
	return true
}

// DispatchEvent adds the unique tags from the TagHandler to the event and passes it to the next stage in the pipeline
func (th *TagHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) error {
	if e.Hostname == "" {
		e.Hostname = string(e.SourceIP)
	}
	e.Tags = uniqueTags(e.Tags, th.tags)
	return th.events.DispatchEvent(ctx, e)
}

// WaitForEvents waits for all event-dispatching goroutines to finish.
func (th *TagHandler) WaitForEvents() {
	th.events.WaitForEvents()
}

var present = struct{}{}

// uniqueTags returns the set of t1 | t2.
func uniqueTags(t1 gostatsd.Tags, t2 gostatsd.Tags) gostatsd.Tags {
	tags := gostatsd.Tags{}
	seen := map[string]struct{}{}

	for _, v := range t1 {
		if _, ok := seen[v]; !ok {
			tags = append(tags, v)
			seen[v] = present
		}
	}

	for _, v := range t2 {
		if _, ok := seen[v]; !ok {
			tags = append(tags, v)
			seen[v] = present
		}
	}

	return tags
}
