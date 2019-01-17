package statsd

import (
	"context"
	"sync"

	"github.com/atlassian/gostatsd"
)

type capturingHandler struct {
	m []*gostatsd.Metric
	e []*gostatsd.Event
}

func (tch *capturingHandler) EstimatedTags() int {
	return 0
}

func (tch *capturingHandler) DispatchMetric(ctx context.Context, m *gostatsd.Metric) {
	tch.m = append(tch.m, m)
}

func (tch *capturingHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	tch.e = append(tch.e, e)
}

func (tch *capturingHandler) WaitForEvents() {
}

type nopHandler struct{}

func (nh *nopHandler) EstimatedTags() int {
	return 0
}

func (nh *nopHandler) DispatchMetric(ctx context.Context, m *gostatsd.Metric) {
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

func (ch *countingHandler) DispatchMetric(ctx context.Context, m *gostatsd.Metric) {
	m.DoneFunc = nil // Clear DoneFunc because it contains non-predictable variable data which interferes with the tests
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.metrics = append(ch.metrics, *m)
}

func (ch *countingHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.events = append(ch.events, *e)
}

func (ch *countingHandler) WaitForEvents() {
}

/*
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
*/
