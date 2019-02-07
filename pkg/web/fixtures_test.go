package web_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/atlassian/gostatsd"

	"github.com/stretchr/testify/require"
)

type capturingHandler struct {
	mu sync.Mutex
	m  []*gostatsd.Metric
	e  []*gostatsd.Event
}

func (ch *capturingHandler) EstimatedTags() int {
	return 0
}

func (ch *capturingHandler) DispatchMetric(ctx context.Context, m *gostatsd.Metric) {
	m.DoneFunc = nil // Clear DoneFunc because it contains non-predictable variable data which interferes with the tests
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.m = append(ch.m, m)
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
