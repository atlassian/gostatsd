package statsd

import (
	"context"
	"fmt"
	"sync"

	"github.com/atlassian/gostatsd"
)

type capturingClusterForwarder struct {
	mu       sync.Mutex
	expected int
	chDone   chan struct{}
	mapsMap  map[string][]*gostatsd.MetricMap
}

func NewCapturingClusterForwarder(expected int) *capturingClusterForwarder {
	ccf := &capturingClusterForwarder{
		expected: expected,
		mapsMap:  map[string][]*gostatsd.MetricMap{},
		chDone:   make(chan struct{}),
	}
	if expected == 0 {
		close(ccf.chDone)
	}
	return ccf
}

func (ccf *capturingClusterForwarder) DispatchMetricMapTo(ctx context.Context, mm *gostatsd.MetricMap, node string) {
	ccf.mu.Lock()
	defer ccf.mu.Unlock()

	maps, ok := ccf.mapsMap[node]
	if ok {
		maps = []*gostatsd.MetricMap{mm}
	} else {
		maps = append(maps, mm)
	}
	ccf.mapsMap[node] = maps
	ccf.expected--
	fmt.Printf("expected=%v\n", ccf.expected)
	if ccf.expected == 0 {
		close(ccf.chDone)
	}
}

func (ccf *capturingClusterForwarder) wait(ctx context.Context) bool {
	fmt.Printf("waiting XXXXX %#v\n", ccf.chDone)
	select {
	case <-ctx.Done():
		return false
	case <-ccf.chDone:
		return true
	}
}

func (ccf *capturingClusterForwarder) Maps() map[string][]*gostatsd.MetricMap {
	ccf.mu.Lock()
	defer ccf.mu.Unlock()
	return ccf.mapsMap // this could allow someone else to modify it, but this is a test fixture only
}

