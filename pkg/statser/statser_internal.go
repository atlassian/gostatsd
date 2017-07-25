package statser

import (
	"context"
	"time"

	"github.com/atlassian/gostatsd"
)

// InternalHandler is an interface to dispatch metrics to.  Exists
// to break circular dependencies.
type InternalHandler interface {
	DispatchMetric(ctx context.Context, m *gostatsd.Metric) error
}

// InternalStatser is a Statser which sends metrics to a handler
type InternalStatser struct {
	ctx       context.Context
	tags      gostatsd.Tags
	namespace string
	hostname  string
	handler   InternalHandler
}

// NewInternalStatser creates a new Statser which sends metrics to the
// supplied InternalHandler
func NewInternalStatser(ctx context.Context, tags gostatsd.Tags, namespace, hostname string, handler InternalHandler) Statser {
	return &InternalStatser{
		ctx:       ctx,
		tags:      tags,
		namespace: namespace,
		hostname:  hostname,
		handler:   handler,
	}
}

// Gauge sends a gauge metric
func (is *InternalStatser) Gauge(name string, value float64, tags gostatsd.Tags) {
	g := &gostatsd.Metric{
		Name:     name,
		Value:    value,
		Tags:     tags,
		Hostname: is.hostname,
		Type:     gostatsd.GAUGE,
	}
	is.dispatchMetric(g)
}

// Count sends a counter metric
func (is *InternalStatser) Count(name string, amount float64, tags gostatsd.Tags) {
	c := &gostatsd.Metric{
		Name:     name,
		Value:    amount,
		Tags:     tags,
		Hostname: is.hostname,
		Type:     gostatsd.COUNTER,
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
		Name:     name,
		Value:    ms,
		Tags:     tags,
		Hostname: is.hostname,
		Type:     gostatsd.TIMER,
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

// WithTags creates a new InternalStatser with additional tags
func (is *InternalStatser) WithTags(tags gostatsd.Tags) Statser {
	return NewInternalStatser(is.ctx, concatTags(is.tags, tags), is.namespace, is.hostname, is.handler)
}

func (is *InternalStatser) dispatchMetric(metric *gostatsd.Metric) {
	// the metric is owned by this file, we can change it freely because we know its origins
	if is.namespace != "" {
		metric.Name = is.namespace + "." + metric.Name
	}
	metric.Tags = append(metric.Tags, is.tags...)
	is.handler.DispatchMetric(is.ctx, metric)
}
