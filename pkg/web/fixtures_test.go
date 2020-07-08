package web_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/atlassian/gostatsd"
)

type capturingHandler struct {
	mu sync.Mutex
	m  []*gostatsd.Metric
	e  []*gostatsd.Event
}

func (ch *capturingHandler) EstimatedTags() int {
	return 0
}

// Wrapper until we can remove it
func (ch *capturingHandler) DispatchMetrics(ctx context.Context, metrics []*gostatsd.Metric) {
	ch.dispatchMetrics(ctx, metrics)
}

func (ch *capturingHandler) dispatchMetrics(ctx context.Context, metrics []*gostatsd.Metric) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	for _, m := range metrics {
		m.DoneFunc = nil // Clear DoneFunc because it contains non-predictable variable data which interferes with the tests
		ch.m = append(ch.m, m)
	}
}

func (ch *capturingHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
	ch.dispatchMetrics(ctx, mm.AsMetrics())
}

func (ch *capturingHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.e = append(ch.e, e)
}

func (ch *capturingHandler) WaitForEvents() {
}

func (ch *capturingHandler) GetMetrics() []*gostatsd.Metric {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	m := make([]*gostatsd.Metric, len(ch.m))
	copy(m, ch.m)
	return m
}

func testContext(t *testing.T) (context.Context, func()) {
	ctxTest, completeTest := context.WithTimeout(context.Background(), 1100*time.Millisecond)
	go func() {
		after := time.NewTimer(1 * time.Second)
		select {
		case <-ctxTest.Done():
			after.Stop()
		case <-after.C:
			require.Fail(t, "test timed out")
		}
	}()
	return ctxTest, completeTest
}
