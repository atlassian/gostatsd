package stats

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd"
)

type countingStatser struct {
	gauges    uint64
	counters  uint64
	reporters uint64
	timers    uint64
	events    uint64
}

func (cs *countingStatser) NotifyFlush(ctx context.Context, d time.Duration) {}

func (cs *countingStatser) RegisterFlush() (ch <-chan time.Duration, unregister func()) {
	return nil, func() {}
}

func (cs *countingStatser) Gauge(name string, value float64, tags gostatsd.Tags) {
	atomic.AddUint64(&cs.gauges, 1)
}

func (cs *countingStatser) Count(name string, amount float64, tags gostatsd.Tags) {
	atomic.AddUint64(&cs.counters, 1)
}

func (cs *countingStatser) Increment(name string, tags gostatsd.Tags) {
	atomic.AddUint64(&cs.counters, 1)
}

func (cs *countingStatser) Report(name string, value float64, tags gostatsd.Tags) {
	atomic.AddUint64(&cs.reporters, 1)
}

func (cs *countingStatser) TimingMS(name string, ms float64, tags gostatsd.Tags) {
	atomic.AddUint64(&cs.timers, 1)
}

func (cs *countingStatser) TimingDuration(name string, d time.Duration, tags gostatsd.Tags) {
	atomic.AddUint64(&cs.timers, 1)
}

func (cs *countingStatser) NewTimer(name string, tags gostatsd.Tags) *Timer {
	return newTimer(cs, name, tags)
}

func (cs *countingStatser) WithTags(tags gostatsd.Tags) Statser {
	return cs
}

func (cs *countingStatser) Event(ctx context.Context, e *gostatsd.Event) {
	atomic.AddUint64(&cs.events, 1)
}

func (cs *countingStatser) WaitForEvents() {}
