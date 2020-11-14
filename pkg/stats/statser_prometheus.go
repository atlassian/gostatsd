package stats

import (
	"context"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/prometheus/client_golang/prometheus"
)

// NOTE: is using a collector/vec of gauges/counters reasonable? Or does
// using only a single gauge/counter suffice?

// this depends on the metrics being collected, if they are the same thing
// but can be partitioned into different types/groups, e.g. http requests partitioned
// by user age and demographics, then using a collector makes sense. (is this correct?)

// PrometheusStatser is a Statser that monitors gostasd's internal metrics from
// Prometheus, it is useful when there is a large number of ephemeral hosts.
type PrometheusStatser struct {
	flushNotifier

	// collector of gauges that stores the internal gauge metrics of gostatsd
	gaugeVec prometheus.GaugeVec
	// collector of counters that stores the internal count metrics of gostatsd
	counterVec prometheus.CounterVec
}

// NewPrometheusStatser creates a new Statser which
// sends internal metrics to prometheus
func NewPrometheusStatser(gaugeVec prometheus.GaugeVec, counterVec prometheus.CounterVec) Statser {
	return &PrometheusStatser{
		gaugeVec: gaugeVec,
		counterVec: counterVec,
	}
}

func (ps *PrometheusStatser) NotifyFlush(ctx context.Context, d time.Duration) {
	ps.flushNotifier.NotifyFlush(ctx, d)
}

func (ps *PrometheusStatser) RegisterFlush() (<-chan time.Duration, func()) {
	return ps.flushNotifier.RegisterFlush()
}

// TODO: how do I use tags here, same for the Count and the TimingMS methods
func (ps *PrometheusStatser) Gauge(name string, value float64, tags gostatsd.Tags) {
	ps.gaugeVec.WithLabelValues(name).Add(value)
}

func (ps *PrometheusStatser) Count(name string, amount float64, tags gostatsd.Tags) {
	ps.counterVec.WithLabelValues(name).Add(amount)
}

func (ps *PrometheusStatser) Increment(name string, tags gostatsd.Tags) {
	ps.Count(name, 1, tags)
}

// TimingMS sends a timing metric from a millisecond value
func (ps *PrometheusStatser) TimingMS(name string, ms float64, tags gostatsd.Tags) {
	ps.counterVec.WithLabelValues(name).Add(ms)
}

// TimingDuration sends a timing metric from a time.Duration
func (ps *PrometheusStatser) TimingDuration(name string, d time.Duration, tags gostatsd.Tags) {
	ps.TimingMS(name, float64(d) / float64(time.Millisecond), tags)
}

// NewTimer returns a new timer with time set to now
func (ps *PrometheusStatser) NewTimer(name string, tags gostatsd.Tags) *Timer {
	return newTimer(ps, name, tags)
}

// WithTags creates a new Statser with additional tags
func (ps *PrometheusStatser) WithTags(tags gostatsd.Tags) Statser {
	return NewTaggedStatser(ps, tags)
}
