package statsd

import (
	"context"
	"sync"

	"github.com/atlassian/gostatsd"
)

type capturingHandler struct {
	mm []*gostatsd.MetricMap
	e  []*gostatsd.Event
}

func (tch *capturingHandler) EstimatedTags() int {
	return 0
}

func (tch *capturingHandler) DispatchMetricMap(ctx context.Context, metrics *gostatsd.MetricMap) {
	tch.mm = append(tch.mm, metrics)
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

func (nh *nopHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
}

func (nh *nopHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
}

func (nh *nopHandler) WaitForEvents() {
}

type expectingHandler struct {
	countingHandler

	wgMetricMaps sync.WaitGroup
	wgEvents     sync.WaitGroup
}

func (e *expectingHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
	e.countingHandler.DispatchMetricMap(ctx, mm)
	e.wgMetricMaps.Done()
}

func (e *expectingHandler) DispatchEvent(ctx context.Context, event *gostatsd.Event) {
	e.countingHandler.DispatchEvent(ctx, event)
	e.wgEvents.Done()
}

func (e *expectingHandler) Expect(mms, es int) {
	e.wgMetricMaps.Add(mms)
	e.wgEvents.Add(es)
}

func (e *expectingHandler) WaitAll() {
	e.wgMetricMaps.Wait()
	e.wgEvents.Wait()
}

type countingHandler struct {
	mu      sync.Mutex
	metrics []*gostatsd.Metric
	maps    []*gostatsd.MetricMap
	events  gostatsd.Events
}

func (ch *countingHandler) Metrics() []*gostatsd.Metric {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	result := make([]*gostatsd.Metric, len(ch.metrics))
	copy(result, ch.metrics)
	return result
}

func (ch *countingHandler) MetricMaps() []*gostatsd.MetricMap {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	result := make([]*gostatsd.MetricMap, len(ch.maps))
	copy(result, ch.maps)
	return result
}

func (ch *countingHandler) Events() gostatsd.Events {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	result := make(gostatsd.Events, len(ch.events))
	copy(result, ch.events)
	return result
}

func (ch *countingHandler) EstimatedTags() int {
	return 0
}

func (ch *countingHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.maps = append(ch.maps, mm)
}

func (ch *countingHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.events = append(ch.events, e)
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
