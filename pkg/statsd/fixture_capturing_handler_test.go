package statsd

import (
	"context"
	"github.com/atlassian/gostatsd"
	"sync"
)

type capturingHandler struct {
	mu sync.Mutex
	expected int
	chDone   chan struct{}

	m  []*gostatsd.Metric
	mm []*gostatsd.MetricMap
	e  []*gostatsd.Event
}

func newCapturingHandler(expected int) *capturingHandler {
	ch := &capturingHandler{
		expected: expected,
		chDone: make(chan struct{}),
	}
	if expected == 0 {
		close(ch.chDone)
	}
	return ch
}

func (ch *capturingHandler) wait(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-ch.chDone:
		return true
	}
}

func (ch *capturingHandler) EstimatedTags() int {
	return 0
}

func (ch *capturingHandler) DispatchMetrics(ctx context.Context, metrics []*gostatsd.Metric) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.m = append(ch.m, metrics...)

	ch.expected--
	if ch.expected == 0 {
		close(ch.chDone)
	}
}

func (ch *capturingHandler) DispatchMetricMap(ctx context.Context, metrics *gostatsd.MetricMap) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.mm = append(ch.mm, metrics)

	ch.expected--
	if ch.expected == 0 {
		close(ch.chDone)
	}
}

func (ch *capturingHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.e = append(ch.e, e)

	ch.expected--
	if ch.expected == 0 {
		close(ch.chDone)
	}
}

func (ch *capturingHandler) WaitForEvents() {
}
