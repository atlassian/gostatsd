package statsd

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/atlassian/gostatsd"
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

// DispatchMetricMap adds the unique tags from the TagHandler to each consolidated metric in the map and passes it to
// the next stage in the pipeline
//
// There is potential to optimize here: if the tagsKey doesn't change, we don't need to re-calculate it.  But we're
// keeping things simple for now.
func (th *TagHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
	mmNew := gostatsd.NewMetricMap(mm.Forwarded)

	mm.Counters.Each(func(metricName, _ string, cOriginal gostatsd.Counter) {
		if th.uniqueFilterAndAddTags(metricName, &cOriginal.Source, &cOriginal.Tags) {
			newTagsKey := gostatsd.FormatTagsKey(cOriginal.Source, cOriginal.Tags)
			if cs, ok := mmNew.Counters[metricName]; ok {
				if cNew, ok := cs[newTagsKey]; ok {
					cNew.Value += cOriginal.Value
					cNew.Timestamp = gostatsd.NanoMax(cNew.Timestamp, cOriginal.Timestamp)
					cs[newTagsKey] = cNew
				} else {
					cs[newTagsKey] = cOriginal
				}
			} else {
				mmNew.Counters[metricName] = map[string]gostatsd.Counter{newTagsKey: cOriginal}
			}
		}
	})

	mm.Gauges.Each(func(metricName, _ string, gOriginal gostatsd.Gauge) {
		if th.uniqueFilterAndAddTags(metricName, &gOriginal.Source, &gOriginal.Tags) {
			newTagsKey := gostatsd.FormatTagsKey(gOriginal.Source, gOriginal.Tags)
			if gs, ok := mmNew.Gauges[metricName]; ok {
				if gNew, ok := gs[newTagsKey]; ok {
					if gOriginal.Timestamp > gNew.Timestamp {
						gNew.Value = gOriginal.Value
						gNew.Timestamp = gOriginal.Timestamp
						gs[newTagsKey] = gNew
					}
				} else {
					gs[newTagsKey] = gOriginal
				}
			} else {
				mmNew.Gauges[metricName] = map[string]gostatsd.Gauge{newTagsKey: gOriginal}
			}
		}
	})

	mm.Timers.Each(func(metricName, _ string, tOriginal gostatsd.Timer) {
		if th.uniqueFilterAndAddTags(metricName, &tOriginal.Source, &tOriginal.Tags) {
			newTagsKey := gostatsd.FormatTagsKey(tOriginal.Source, tOriginal.Tags)
			if ts, ok := mmNew.Timers[metricName]; ok {
				if tNew, ok := ts[newTagsKey]; ok {
					tNew.Values = append(tNew.Values, tOriginal.Values...)
					tNew.Timestamp = gostatsd.NanoMax(tNew.Timestamp, tOriginal.Timestamp)
					tNew.SampledCount += tOriginal.SampledCount
					ts[newTagsKey] = tNew
				} else {
					ts[newTagsKey] = tOriginal
				}
			} else {
				mmNew.Timers[metricName] = map[string]gostatsd.Timer{newTagsKey: tOriginal}
			}
		}
	})

	mm.Sets.Each(func(metricName, _ string, sOriginal gostatsd.Set) {
		if th.uniqueFilterAndAddTags(metricName, &sOriginal.Source, &sOriginal.Tags) {
			newTagsKey := gostatsd.FormatTagsKey(sOriginal.Source, sOriginal.Tags)
			if ss, ok := mmNew.Sets[metricName]; ok {
				if sNew, ok := ss[newTagsKey]; ok {
					for key := range sOriginal.Values {
						sNew.Values[key] = struct{}{}
					}
					sNew.Timestamp = gostatsd.NanoMax(sNew.Timestamp, sOriginal.Timestamp)
					ss[newTagsKey] = sNew
				} else {
					ss[newTagsKey] = sOriginal
				}
			} else {
				mmNew.Sets[metricName] = map[string]gostatsd.Set{newTagsKey: sOriginal}
			}
		}
	})

	if !mmNew.IsEmpty() {
		th.handler.DispatchMetricMap(ctx, mmNew)
	}
}

// uniqueFilterAndAddTags will perform 3 tasks:
// - Add static tags configured to the metric
// - De-duplicate tags
// - Perform rule based filtering
//
// Everything is done in one function for efficiency, as the steps listed above are interrelated, and this is on the
// hot code path.
//
// Returns true if the metric should be processed further, or false to drop it.
func (th *TagHandler) uniqueFilterAndAddTags(mName string, mHostname *gostatsd.Source, mTags *gostatsd.Tags) bool {
	if len(th.filters) == 0 {
		*mTags = uniqueTags(*mTags, th.tags)
		return true
	}

	dropTags := map[string]struct{}{}

	for _, filter := range th.filters {
		if len(filter.MatchMetrics) > 0 && !filter.MatchMetrics.MatchAny(mName) { // returns false if nothing present
			// name doesn't match an include, stop
			continue
		}

		// this list may be empty, and therefore return false
		if filter.ExcludeMetrics.MatchAny(mName) { // returns false if nothing present
			// name matches an exclude, stop
			continue
		}

		if len(filter.MatchTags) > 0 && !filter.MatchTags.MatchAnyMultiple(*mTags) { // returns false if either list is empty
			// no tags match
			continue
		}

		if filter.DropMetric {
			return false
		}

		for _, dropFilter := range filter.DropTags {
			for _, tag := range *mTags {
				if dropFilter.Match(tag) {
					dropTags[tag] = present
				}
			}
		}

		if filter.DropHost {
			*mHostname = ""
		}
	}

	*mTags = uniqueTagsWithSeen(dropTags, *mTags, th.tags)
	return true
}

// DispatchEvent adds the unique tags from the TagHandler to the event and passes it to the next stage in the pipeline
func (th *TagHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
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
