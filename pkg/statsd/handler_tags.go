package statsd

import (
	"context"

	"github.com/atlassian/gostatsd"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type TagHandler struct {
	handler       gostatsd.PipelineHandler
	tags          gostatsd.Tags // Tags to add to all metrics
	filters       []Filter
	estimatedTags int
}

var present = struct{}{}

func NewTagHandlerFromViper(v *viper.Viper, handler gostatsd.PipelineHandler, tags gostatsd.Tags) *TagHandler {
	filterNameList := v.GetStringSlice("filters")
	var filters []Filter
	for _, filterName := range filterNameList {
		vFilter := v.Sub("filter." + filterName)
		if vFilter == nil {
			logrus.Warnf("Filter doesn't exist: %v", filterName)
			continue
		}
		filters = append(filters, NewFilterFromViper(vFilter))
		logrus.Infof("Loaded filter %v", filterName)
	}
	return NewTagHandler(handler, tags, filters)
}

// NewTagHandler initialises a new handler which adds unique tags, and sends metrics/events to the next handler based
// on filter rules.
func NewTagHandler(handler gostatsd.PipelineHandler, tags gostatsd.Tags, filters []Filter) *TagHandler {
	tags = uniqueTags(tags, gostatsd.Tags{}) // de-dupe tags
	return &TagHandler{
		handler:       handler,
		tags:          tags,
		filters:       filters,
		estimatedTags: len(tags) + handler.EstimatedTags(),
	}
}

// EstimatedTags returns a guess for how many tags to pre-allocate
func (th *TagHandler) EstimatedTags() int {
	return th.estimatedTags
}

// DispatchMetric adds the unique tags from the TagHandler to the metric and passes it to the next stage in the pipeline
func (th *TagHandler) DispatchMetric(ctx context.Context, m *gostatsd.Metric) {
	if m.Hostname == "" {
		m.Hostname = string(m.SourceIP)
	}
	if th.uniqueFilterMetricAndAddTags(m) {
		th.handler.DispatchMetric(ctx, m)
	}
}

// uniqueFilterMetricAndAddTags will perform 3 tasks:
// - Add static tags configured to the metric
// - De-duplicate tags
// - Perform rule based filtering
//
// Everything is done in one function for efficiency, as the steps listed above are interrelated, and this is on the
// hot code path.
//
// Returns true if the metric should be processed further, or false to drop it.
func (th *TagHandler) uniqueFilterMetricAndAddTags(m *gostatsd.Metric) bool {
	if len(th.filters) == 0 {
		m.Tags = uniqueTags(m.Tags, th.tags)
		return true
	}

	dropTags := map[string]struct{}{}

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

		for _, dropFilter := range filter.DropTags {
			for _, tag := range m.Tags {
				if dropFilter.Match(tag) {
					dropTags[tag] = present
				}
			}
		}

		if filter.DropHost {
			m.Hostname = ""
		}
	}

	m.Tags = uniqueTagsWithSeen(dropTags, m.Tags, th.tags)
	return true
}

// DispatchEvent adds the unique tags from the TagHandler to the event and passes it to the next stage in the pipeline
func (th *TagHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	if e.Hostname == "" {
		e.Hostname = string(e.SourceIP)
	}
	e.Tags = uniqueTags(e.Tags, th.tags)
	th.handler.DispatchEvent(ctx, e)
}

// WaitForEvents waits for all event-dispatching goroutines to finish.
func (th *TagHandler) WaitForEvents() {
	th.handler.WaitForEvents()
}

// uniqueTags returns the set of t1 | t2.  It may modify the contents of t1 and t2.
func uniqueTags(t1 gostatsd.Tags, t2 gostatsd.Tags) gostatsd.Tags {
	return uniqueTagsWithSeen(map[string]struct{}{}, t1, t2)
}

// uniqueTags returns the set of (t1 | t2) - seen.  It may modify the contents of t1, t2, and seen.
func uniqueTagsWithSeen(seen map[string]struct{}, t1 gostatsd.Tags, t2 gostatsd.Tags) gostatsd.Tags {
	last := len(t1)
	for idx := 0; idx < last; {
		tag := t1[idx]
		if _, ok := seen[tag]; ok {
			last--
			t1[idx] = t1[last]
			t1 = t1[:last]
		} else {
			seen[tag] = present
			idx++
		}
	}

	for _, tag := range t2 {
		if _, ok := seen[tag]; !ok {
			t1 = append(t1, tag)
		}
	}

	return t1
}
