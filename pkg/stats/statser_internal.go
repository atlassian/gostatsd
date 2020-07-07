package stats

import (
	"context"
	"sync/atomic"
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

	buffer chan *gostatsd.Metric

	tags      gostatsd.Tags
	namespace string
	hostname  gostatsd.Source
	handler   gostatsd.PipelineHandler
	dropped   uint64
}

const bufferSize = 1000 // estimating this is difficult and tends to cause problems if too small

// NewInternalStatser creates a new Statser which sends metrics to the
// supplied InternalHandler.
func NewInternalStatser(tags gostatsd.Tags, namespace string, hostname gostatsd.Source, handler gostatsd.PipelineHandler) *InternalStatser {
	return &InternalStatser{
		buffer:    make(chan *gostatsd.Metric, bufferSize),
		tags:      tags,
		namespace: namespace,
		hostname:  hostname,
		handler:   handler,
	}
}

// Run will pull internal metrics off a small buffer, and dispatch them.  It
// stops running when the context is closed.
func (is *InternalStatser) Run(ctx context.Context) {
	flushed, unregister := is.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case m := <-is.buffer:
			is.dispatchMetric(ctx, m)
		case <-flushed:
			is.Gauge("internal_dropped", float64(atomic.LoadUint64(&is.dropped)), nil)
		}
	}
}

// Gauge sends a gauge metric
func (is *InternalStatser) Gauge(name string, value float64, tags gostatsd.Tags) {
	g := &gostatsd.Metric{
		Name:     name,
		Value:    value,
		Tags:     tags,
		Hostname: is.hostname,
		Rate:     1,
		Type:     gostatsd.GAUGE,
	}
	is.dispatchInternal(g)
}

// Count sends a counter metric
func (is *InternalStatser) Count(name string, amount float64, tags gostatsd.Tags) {
	c := &gostatsd.Metric{
		Name:     name,
		Value:    amount,
		Tags:     tags,
		Hostname: is.hostname,
		Rate:     1,
		Type:     gostatsd.COUNTER,
	}
	is.dispatchInternal(c)
}

// Increment sends a counter metric with a value of 1
func (is *InternalStatser) Increment(name string, tags gostatsd.Tags) {
	is.Count(name, 1, tags)
}

// TimingMS sends a timing metric from a millisecond value
func (is *InternalStatser) TimingMS(name string, ms float64, tags gostatsd.Tags) {
	c := &gostatsd.Metric{
		Name:     name,
		Value:    ms,
		Tags:     tags,
		Hostname: is.hostname,
		Rate:     1,
		Type:     gostatsd.TIMER,
	}
	is.dispatchInternal(c)
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

// Attempts to dispatch a metric via the internal buffer.  Non-blocking.
// Failure to send will be tracked, but not propagated to the caller.
func (is *InternalStatser) dispatchInternal(metric *gostatsd.Metric) {
	metric.Timestamp = gostatsd.NanoNow()

	select {
	case is.buffer <- metric:
		// great success
	default:
		// at least we tried
		atomic.AddUint64(&is.dropped, 1)
	}
}

func (is *InternalStatser) dispatchMetric(ctx context.Context, metric *gostatsd.Metric) {
	// the metric is owned by this file, we can change it freely because we know its origins
	if is.namespace != "" {
		metric.Name = is.namespace + "." + metric.Name
	}
	metric.Tags = metric.Tags.Concat(is.tags)
	is.handler.DispatchMetrics(ctx, []*gostatsd.Metric{metric})
}
