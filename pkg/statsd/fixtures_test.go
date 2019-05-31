package statsd

import (
	"context"
	"sync"

	"github.com/atlassian/gostatsd"
)

type nopHandler struct{}

func (nh *nopHandler) EstimatedTags() int {
	return 0
}

func (nh *nopHandler) DispatchMetrics(ctx context.Context, m []*gostatsd.Metric) {
}

func (nh *nopHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
}

func (nh *nopHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
}

func (nh *nopHandler) WaitForEvents() {
}

type countingHandler struct {
	mu      sync.Mutex
	metrics []gostatsd.Metric
	events  gostatsd.Events
}

func (ch *countingHandler) EstimatedTags() int {
	return 0
}

func (ch *countingHandler) DispatchMetrics(ctx context.Context, metrics []*gostatsd.Metric) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	for _, m := range metrics {
		m.DoneFunc = nil // Clear DoneFunc because it contains non-predictable variable data which interferes with the tests
		ch.metrics = append(ch.metrics, *m)
	}
}

// DispatchMetricMap re-dispatches a metric map through BackendHandler.DispatchMetrics
func (ch *countingHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
	mm.DispatchMetrics(ctx, ch)
}

func (ch *countingHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.events = append(ch.events, *e)
}

func (ch *countingHandler) WaitForEvents() {
}

