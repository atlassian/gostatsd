package gostatsd

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testContext returns a context that will timeout and fail the test if not canceled.  Used to
// enforce a timeout on tests.
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

type capturingHandler struct {
	mu sync.Mutex
	m  []*Metric
	e  []*Event
}

func (ch *capturingHandler) EstimatedTags() int {
	return 0
}

func (ch *capturingHandler) DispatchMetric(ctx context.Context, m *Metric) {
	m.DoneFunc = nil // Clear DoneFunc because it contains non-predictable variable data which interferes with the tests
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.m = append(ch.m, m)
}

func (ch *capturingHandler) DispatchEvent(ctx context.Context, e *Event) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.e = append(ch.e, e)
}

func (ch *capturingHandler) WaitForEvents() {
}

func (ch *capturingHandler) GetMetrics() []*Metric {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	m := make([]*Metric, len(ch.m))
	copy(m, ch.m)
	return m
}
