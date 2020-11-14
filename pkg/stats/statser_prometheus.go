package stats

import (
	"context"
	"time"
)

// PrometheusStatser is a Statser that monitors gostasd's internal metrics from
// Prometheus, it is useful when there is a large number of ephemeral hosts.
type PrometheusStatser struct {
	flushNotifier
}

func (ps *PrometheusStatser) NotifyFlush(ctx context.Context, d time.Duration) {
	ps.flushNotifier.NotifyFlush(ctx, d)
}

func (ps *PrometheusStatser) RegisterFlush() (<-chan time.Duration, func()) {
	return ps.flushNotifier.RegisterFlush()
}
