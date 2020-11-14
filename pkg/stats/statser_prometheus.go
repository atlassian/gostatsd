package stats

import (
	"context"
	"github.com/atlassian/gostatsd"
	"time"
)

// PrometheusStatser is a Statser that monitors gostasd's internal metrics from
// Prometheus, it is useful when there is a large number of ephemeral hosts.
type PrometheusStatser struct {
	flushNotifier
}

// NewPrometheusStatser creates a new Statser which
// sends internal metrics to prometheus
func NewPrometheusStatser() Statser {
	return &PrometheusStatser{}
}

func (ps *PrometheusStatser) NotifyFlush(ctx context.Context, d time.Duration) {
	ps.flushNotifier.NotifyFlush(ctx, d)
}

func (ps *PrometheusStatser) RegisterFlush() (<-chan time.Duration, func()) {
	return ps.flushNotifier.RegisterFlush()
}

func (ps *PrometheusStatser) Gauge(name string, value float64, tags gostatsd.Tags) {}

func (ps *PrometheusStatser) Count(name string, amount float64, tags gostatsd.Tags) {}

func (ps *PrometheusStatser) Increment(name string, tags gostatsd.Tags) {}

func (ps *PrometheusStatser) TimingMS(name string, ms float64, tags gostatsd.Tags) {}

func (ps *PrometheusStatser) TimingDuration(name string, d time.Duration, tags gostatsd.Tags) {}

// NewTimer returns a new timer with time set to now
func (ps *PrometheusStatser) NewTimer(name string, tags gostatsd.Tags) *Timer {
	return newTimer(ps, name, tags)
}

// WithTags creates a new Statser with additional tags
func (ps *PrometheusStatser) WithTags(tags gostatsd.Tags) Statser {
	return NewTaggedStatser(ps, tags)
}
