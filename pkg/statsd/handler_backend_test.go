package statsd

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"

	"github.com/ash2k/stager/wait"
	"github.com/stretchr/testify/assert"
)

type testAggregator struct {
	agrNumber int
	af        *testAggregatorFactory
	gostatsd.MetricMap
}

func (a *testAggregator) TrackMetrics(statser stats.Statser) {
}

func (a *testAggregator) Receive(m ...*gostatsd.Metric) {
	a.af.Mutex.Lock()
	defer a.af.Mutex.Unlock()
	a.af.receiveInvocations[a.agrNumber] += len(m)
}

func (a *testAggregator) ReceiveMap(mm *gostatsd.MetricMap) {
	a.af.Mutex.Lock()
	defer a.af.Mutex.Unlock()
	a.af.receiveMapInvocations[a.agrNumber]++
}

func (a *testAggregator) Flush(interval time.Duration) {
	a.af.Mutex.Lock()
	defer a.af.Mutex.Unlock()
	a.af.flushInvocations[a.agrNumber]++
}

func (a *testAggregator) Process(f ProcessFunc) {
	a.af.Mutex.Lock()
	defer a.af.Mutex.Unlock()
	a.af.processInvocations[a.agrNumber]++
	f(&a.MetricMap)
}

func (a *testAggregator) Reset() {
	a.af.Mutex.Lock()
	defer a.af.Mutex.Unlock()
	a.af.resetInvocations[a.agrNumber]++
}

type testAggregatorFactory struct {
	sync.Mutex
	receiveInvocations    map[int]int
	receiveMapInvocations map[int]int
	flushInvocations      map[int]int
	processInvocations    map[int]int
	resetInvocations      map[int]int
	numAgrs               int
}

func (af *testAggregatorFactory) Create() Aggregator {
	agrNumber := af.numAgrs
	af.numAgrs++
	af.receiveInvocations[agrNumber] = 0
	af.receiveMapInvocations[agrNumber] = 0
	af.flushInvocations[agrNumber] = 0
	af.processInvocations[agrNumber] = 0
	af.resetInvocations[agrNumber] = 0

	agr := testAggregator{
		agrNumber: agrNumber,
		af:        af,
	}
	agr.Counters = gostatsd.Counters{}
	agr.Timers = gostatsd.Timers{}
	agr.Gauges = gostatsd.Gauges{}
	agr.Sets = gostatsd.Sets{}

	return &agr
}

func newTestFactory() *testAggregatorFactory {
	return &testAggregatorFactory{
		receiveInvocations:    make(map[int]int),
		receiveMapInvocations: make(map[int]int),
		flushInvocations:      make(map[int]int),
		processInvocations:    make(map[int]int),
		resetInvocations:      make(map[int]int),
	}
}

func TestNewBackendHandlerShouldCreateCorrectNumberOfWorkers(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	n := r.Intn(5) + 1
	factory := newTestFactory()
	h := NewBackendHandler(nil, 0, n, 1, factory)
	assert.Equal(t, n, len(h.workers))
	assert.Equal(t, n, factory.numAgrs)
}

func TestRunShouldReturnWhenContextCancelled(t *testing.T) {
	t.Parallel()
	h := NewBackendHandler(nil, 0, 5, 1, newTestFactory())
	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	h.Run(ctx)
}

func TestDispatchMetricsShouldDistributeMetrics(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	n := r.Intn(5) + 1
	factory := newTestFactory()
	// use a sync channel (perWorkerBufferSize = 0) to force the workers to process events before the context is cancelled
	h := NewBackendHandler(nil, 0, n, 0, factory)
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	var wgFinish wait.Group
	wgFinish.StartWithContext(ctx, h.Run)
	numMetrics := r.Intn(1000) + n*10
	var wg sync.WaitGroup
	wg.Add(numMetrics)
	for i := 0; i < numMetrics; i++ {
		m := &gostatsd.Metric{
			Type:  gostatsd.COUNTER,
			Name:  fmt.Sprintf("counter.metric.%d", r.Int63()),
			Tags:  nil,
			Value: r.Float64(),
		}
		go func() {
			defer wg.Done()
			h.DispatchMetrics(ctx, []*gostatsd.Metric{m})
		}()
	}
	wg.Wait()       // Wait for all metrics to be dispatched
	cancelFunc()    // After all metrics have been dispatched, we signal dispatcher to shut down
	wgFinish.Wait() // Wait for dispatcher to shutdown

	receiveInvocations := getTotalInvocations(factory.receiveInvocations)
	assert.Equal(t, numMetrics, receiveInvocations)
	for agrNum, count := range factory.receiveInvocations {
		if count == 0 {
			t.Errorf("aggregator %d was never invoked", agrNum)
		} else {
			t.Logf("aggregator %d was invoked %d time(s)", agrNum, count)
		}
	}
}

func TestDispatchMetricMapShouldDistributeMetrics(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	numAggregators := r.Intn(5) + 1
	factory := newTestFactory()
	// use a sync channel (perWorkerBufferSize = 0) to force the workers to process events before the context is cancelled
	h := NewBackendHandler(nil, 0, numAggregators, 0, factory)
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	var wgFinish wait.Group
	wgFinish.StartWithContext(ctx, h.Run)

	mm := gostatsd.NewMetricMap()
	for i := 0; i < numAggregators*100; i++ {
		m := &gostatsd.Metric{
			Type:  gostatsd.COUNTER,
			Name:  fmt.Sprintf("counter.metric.%d", r.Int63()),
			Tags:  nil,
			Value: r.Float64(),
		}
		m.TagsKey = m.FormatTagsKey()
		mm.Receive(m)
	}

	h.DispatchMetricMap(ctx, mm)

	cancelFunc()    // After dispatch, we signal dispatcher to shut down
	wgFinish.Wait() // Wait for dispatcher to shutdown

	for agrNum, count := range factory.receiveMapInvocations {
		assert.NotZerof(t, count, "aggregator=%d", agrNum)
		if count == 0 {
			t.Errorf("aggregator %d was never invoked", agrNum)
			for idx, mmSplit := range mm.Split(numAggregators) {
				fmt.Printf("aggr %d, names %d\n", idx, len(mmSplit.Counters))
			}
		} else {
			t.Logf("aggregator %d was invoked %d time(s)", agrNum, count)
		}
	}
}

func getTotalInvocations(inv map[int]int) int {
	var counter int
	for _, i := range inv {
		counter += i
	}
	return counter
}

func BenchmarkBackendHandler(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	factory := newTestFactory()
	h := NewBackendHandler(nil, 0, runtime.NumCPU(), 10, factory)
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	var wgFinish wait.Group
	wgFinish.StartWithContext(ctx, h.Run)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := &gostatsd.Metric{
				Type:  gostatsd.COUNTER,
				Name:  fmt.Sprintf("counter.metric.%d", rand.Int63()),
				Tags:  nil,
				Value: rand.Float64(),
			}
			h.DispatchMetrics(ctx, []*gostatsd.Metric{m})
		}
	})
	cancelFunc()    // After all metrics have been dispatched, we signal dispatcher to shut down
	wgFinish.Wait() // Wait for dispatcher to shutdown
}
