package web_test

import (
	"context"
	"sync"
	"time"

	"github.com/atlassian/gostatsd"
)

type capturingHandler struct {
	mu   sync.Mutex
	maps []*gostatsd.MetricMap
	e    []*gostatsd.Event
}

func (ch *capturingHandler) EstimatedTags() int {
	return 0
}

func (ch *capturingHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.maps = append(ch.maps, mm)
}

func (ch *capturingHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.e = append(ch.e, e)
}

func (ch *capturingHandler) WaitForEvents() {
}

func (ch *capturingHandler) MetricMaps() []*gostatsd.MetricMap {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	mms := make([]*gostatsd.MetricMap, len(ch.maps))
	copy(mms, ch.maps)
	return mms
}

func testContext() (context.Context, func()) {
	ctxTest, completeTest := context.WithTimeout(context.Background(), 1100*time.Millisecond)
	go func() {
		after := time.NewTimer(1 * time.Second)
		select {
		case <-ctxTest.Done():
			after.Stop()
		case <-after.C:
			panic("test timed out")
		}
	}()
	return ctxTest, completeTest
}
