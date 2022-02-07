package stats

import (
	"context"
	"time"

	"github.com/atlassian/gostatsd"
)

// InternalStatser is a Statser which sends metrics to a handler on a best
// effort basis.  If all buffers are full, metrics will be dropped.  Dropped
// metrics will be accumulated and emitted as a gauge (not counter).  Metrics
// sent after the context is closed will be counted as dropped, but never
// surfaced because it has nowhere to submit them.
//
// There is an assumption (but not enforcement) that InternalStatser is a
// singleton, and therefore there is no namespacing/tags on the dropped metrics.
type InternalStatser struct {
	flushNotifier

	tags      gostatsd.Tags
	namespace string
	hostname  gostatsd.Source
	handler   gostatsd.PipelineHandler

	consolidator *gostatsd.MetricConsolidator
}

// NewInternalStatser creates a new Statser which sends metrics to the
// supplied InternalHandler.
func NewInternalStatser(tags gostatsd.Tags, namespace string, hostname gostatsd.Source, handler gostatsd.PipelineHandler) *InternalStatser {
	if hostname != gostatsd.UnknownSource {
		tags = tags.Concat(gostatsd.Tags{"host:" + string(hostname)})
	}
	return &InternalStatser{
		tags:      tags,
		namespace: namespace,
		hostname:  hostname,
		handler:   handler,
		// We can't just use a MetricMap because everything
		// that writes to it is on its own goroutine.
		consolidator: gostatsd.NewMetricConsolidator(10, false, 0, nil),
	}
}

func (is *InternalStatser) NotifyFlush(ctx context.Context, d time.Duration) {
	mms := is.consolidator.DrainWithContext(ctx)
	if mms == nil {
		// context is canceled
		return
	}
	is.consolidator.Fill()
	is.handler.DispatchMetricMap(ctx, gostatsd.MergeMaps(mms))
	is.flushNotifier.NotifyFlush(ctx, d)

}

// Gauge sends a gauge metric
func (is *InternalStatser) Gauge(name string, value float64, tags gostatsd.Tags) {
	g := &gostatsd.Metric{
		Name:   name,
		Value:  value,
		Tags:   tags,
		Source: is.hostname,
		Rate:   1,
		Type:   gostatsd.GAUGE,
	}
	is.dispatchMetric(g)
}

// Count sends a counter metric
func (is *InternalStatser) Count(name string, amount float64, tags gostatsd.Tags) {
	c := &gostatsd.Metric{
		Name:   name,
		Value:  amount,
		Tags:   tags,
		Source: is.hostname,
		Rate:   1,
		Type:   gostatsd.COUNTER,
	}
	is.dispatchMetric(c)
}

// Increment sends a counter metric with a value of 1
func (is *InternalStatser) Increment(name string, tags gostatsd.Tags) {
	is.Count(name, 1, tags)
}

// TimingMS sends a timing metric from a millisecond value
func (is *InternalStatser) TimingMS(name string, ms float64, tags gostatsd.Tags) {
	c := &gostatsd.Metric{
		Name:   name,
		Value:  ms,
		Tags:   tags,
		Source: is.hostname,
		Rate:   1,
		Type:   gostatsd.TIMER,
	}
	is.dispatchMetric(c)
}

// TimingDuration sends a timing metric from a time.Duration
func (is *InternalStatser) TimingDuration(name string, d time.Duration, tags gostatsd.Tags) {
	is.TimingMS(name, float64(d)/float64(time.Millisecond), tags)
}

// NewTimer returns a new timer with time set to now
func (is *InternalStatser) NewTimer(name string, tags gostatsd.Tags) *Timer {
	return newTimer(is, name, tags)
}

// WithTags creates a new Statser with additional tags
func (is *InternalStatser) WithTags(tags gostatsd.Tags) Statser {
	return NewTaggedStatser(is, tags)
}

func (is *InternalStatser) dispatchMetric(metric *gostatsd.Metric) {
	// the metric is owned by this file, we can change it freely because we know its origins
	if is.namespace != "" {
		metric.Name = is.namespace + "." + metric.Name
	}
	metric.Tags = metric.Tags.Concat(is.tags)
	is.consolidator.ReceiveMetrics([]*gostatsd.Metric{metric})
}

func (is *InternalStatser) Event(ctx context.Context, e *gostatsd.Event) {
	is.handler.DispatchEvent(ctx, e)
}

func (is *InternalStatser) WaitForEvents() {
	is.handler.WaitForEvents()
}
