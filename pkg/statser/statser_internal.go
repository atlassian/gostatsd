package statser

import (
	"context"
	"sync"
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
	ctx  context.Context
	lock sync.RWMutex
	wg   *sync.WaitGroup

	tags      gostatsd.Tags
	namespace string
	hostname  string
	handler   InternalHandler
}

// NewInternalStatser creates a new Statser which sends metrics to the
// supplied InternalHandler.  The WaitGroup must be waited on after
// the context is closed.
func NewInternalStatser(ctx context.Context, wg *sync.WaitGroup, tags gostatsd.Tags, namespace, hostname string, handler InternalHandler) Statser {
	is := &InternalStatser{
		ctx:       ctx,
		wg:        wg,
		tags:      tags,
		namespace: namespace,
		hostname:  hostname,
		handler:   handler,
	}
	wg.Add(1)
	go is.run()
	return is
}

func (is *InternalStatser) run() {
	select {
	case <-is.ctx.Done():
		// At this point we're certain the Context is closed.  Wait for
		// all consumers to release their locks.  On the next dispatch,
		// they will see the closed Context and not attempt to proceed.
		is.lock.Lock()
		is.lock.Unlock()
		is.wg.Done()
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
	return NewInternalStatser(is.ctx, is.wg, concatTags(is.tags, tags), is.namespace, is.hostname, is.handler)
}

func (is *InternalStatser) dispatchMetric(metric *gostatsd.Metric) {
	is.lock.RLock()
	defer is.lock.RUnlock()

	select {
	case <-is.ctx.Done():
		return
	default:
	}

	// the metric is owned by this file, we can change it freely because we know its origins
	if is.namespace != "" {
		metric.Name = is.namespace + "." + metric.Name
	}
	metric.Tags = append(metric.Tags, is.tags...)
	_ = is.handler.DispatchMetric(is.ctx, metric)
}
