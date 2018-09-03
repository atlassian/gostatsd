package statsd

import (
	"context"
	"sync"

	"github.com/atlassian/gostatsd"
)

type TagCapturingHandler struct {
	m []*gostatsd.Metric
	e []*gostatsd.Event
}

func (tch *TagCapturingHandler) EstimatedTags() int {
	return 0
}

func (tch *TagCapturingHandler) DispatchMetric(ctx context.Context, m *gostatsd.Metric) error {
	tch.m = append(tch.m, m)
	return nil
}

func (tch *TagCapturingHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) error {
	tch.e = append(tch.e, e)
	return nil
}

func (tch *TagCapturingHandler) WaitForEvents() {
}

type nopHandler struct{}

func (nh *nopHandler) EstimatedTags() int {
	return 0
}

func (nh *nopHandler) DispatchMetric(ctx context.Context, m *gostatsd.Metric) error {
	return nil
}

func (nh *nopHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) error {
	return nil
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

func (ch *countingHandler) DispatchMetric(ctx context.Context, m *gostatsd.Metric) error {
	m.DoneFunc = nil // Clear DoneFunc because it contains non-predictable variable data which interferes with the tests
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.metrics = append(ch.metrics, *m)
	return nil
}

func (ch *countingHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.events = append(ch.events, *e)
	return nil
}

func (ch *countingHandler) WaitForEvents() {
}
