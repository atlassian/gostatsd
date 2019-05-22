package statsd

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ash2k/stager/wait"
	"github.com/sirupsen/logrus"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"
)

// AggregatorFactory creates Aggregator objects.
type AggregatorFactory interface {
	// Create creates Aggregator objects.
	Create() Aggregator
}

// AggregatorFactoryFunc type is an adapter to allow the use of ordinary functions as AggregatorFactory.
type AggregatorFactoryFunc func() Aggregator

// Create calls f().
func (f AggregatorFactoryFunc) Create() Aggregator {
	return f()
}

// BackendEventHandler dispatches metrics and events to all configured backends (via Aggregators)
type BackendHandler struct {
	eventWg          sync.WaitGroup
	backends         []gostatsd.Backend
	concurrentEvents chan struct{}

	numWorkers int
	workers    []*worker
}

// NewBackendHandler initialises a new Handler which sends metrics and events to all backends
func NewBackendHandler(backends []gostatsd.Backend, maxConcurrentEvents uint, numWorkers int, perWorkerBufferSize int, af AggregatorFactory) *BackendHandler {
	workers := make([]*worker, numWorkers)

	for i := 0; i < numWorkers; i++ {
		workers[i] = &worker{
			aggr: af.Create(),
			// TODO: Reassess the defaults
			metricsQueue:   make(chan []*gostatsd.Metric, perWorkerBufferSize),
			metricMapQueue: make(chan *gostatsd.MetricMap, perWorkerBufferSize),
			processChan:    make(chan *processCommand),
			id:             i,
		}
	}

	return &BackendHandler{
		backends:         backends,
		concurrentEvents: make(chan struct{}, maxConcurrentEvents),

		numWorkers: numWorkers,
		workers:    workers,
	}
}

// Run runs the BackendHandler workers until the Context is closed.
func (bh *BackendHandler) Run(ctx context.Context) {
	var wg wait.Group
	defer func() {
		for _, worker := range bh.workers {
			close(worker.metricsQueue)   // Close channel to terminate worker
			close(worker.metricMapQueue) // Close channel to terminate worker
		}
		wg.Wait() // Wait for all workers to finish
	}()
	for _, worker := range bh.workers {
		wg.Start(worker.work)
	}

	// Work until asked to stop
	<-ctx.Done()
}

// RunMetricsContext pulls a Statser from the Context and invokes RunMetrics.  Allows a BackendHandler to still
// conform to MetricEmitter.
func (bh *BackendHandler) RunMetricsContext(ctx context.Context) {
	bh.RunMetrics(ctx, stats.FromContext(ctx))
}

// RunMetrics attaches a Statser to the BackendHandler.  Stops when the context is closed.
func (bh *BackendHandler) RunMetrics(ctx context.Context, statser stats.Statser) {
	var wg wait.Group
	defer wg.Wait()

	// Starts the metrics for workers
	for _, worker := range bh.workers {
		worker := worker
		wg.Start(func() {
			worker.RunMetrics(ctx, statser)
		})
	}

	// Starts the metrics for aggregators
	bh.Process(ctx, func(aggrId int, aggr Aggregator) {
		if me, ok := aggr.(MetricEmitter); ok {
			tag := fmt.Sprintf("aggregator_id:%d", aggrId)
			me.RunMetrics(ctx, statser.WithTags(gostatsd.Tags{tag}))
		}
	})

	// Starts the CSW for events
	csw := stats.NewChannelStatsWatcher(
		statser,
		"backend_events_sem",
		nil,
		cap(bh.concurrentEvents),
		func() int { return len(bh.concurrentEvents) },
		time.Second,
	)
	wg.StartWithContext(ctx, csw.Run)
}

// EstimatedTags returns a guess for how many tags to pre-allocate
func (bh *BackendHandler) EstimatedTags() int {
	return 0
}

// DispatchMetrics dispatches metric to a corresponding Aggregator.
func (bh *BackendHandler) DispatchMetrics(ctx context.Context, metrics []*gostatsd.Metric) {
	metricsByAggr := make([][]*gostatsd.Metric, bh.numWorkers)

	for _, m := range metrics {
		m.TagsKey = m.FormatTagsKey() // this is expensive, so do it with no aggregator affinity
		bucket := m.Bucket(bh.numWorkers)
		metricsByAggr[bucket] = append(metricsByAggr[bucket], m)
	}

	for aggrIdx, bucketedMetrics := range metricsByAggr {
		w := bh.workers[aggrIdx]
		select {
		case <-ctx.Done():
		case w.metricsQueue <- bucketedMetrics:
		}
	}
}

// DispatchMetricMap re-dispatches a metric map through BackendHandler.DispatchMetrics
func (bh *BackendHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
	maps := mm.Split(bh.numWorkers)

	for aggrIdx, mmSplit := range maps {
		if !mm.IsEmpty() {
			w := bh.workers[aggrIdx]
			select {
			case <-ctx.Done():
			case w.metricMapQueue <- mmSplit:
			}
		}
	}
}

// Process concurrently executes provided function in goroutines that own Aggregators.
// DispatcherProcessFunc function may be executed zero or up to numWorkers times. It is executed
// less than numWorkers times if the context signals "done".
func (bh *BackendHandler) Process(ctx context.Context, f DispatcherProcessFunc) gostatsd.Wait {
	var wg sync.WaitGroup
	cmd := &processCommand{
		f:    f,
		done: wg.Done,
	}
	wg.Add(bh.numWorkers)
	cmdSent := 0
loop:
	for _, worker := range bh.workers {
		select {
		case <-ctx.Done():
			wg.Add(cmdSent - bh.numWorkers) // Not all commands have been sent, should decrement the WG counter.
			break loop
		case worker.processChan <- cmd:
			cmdSent++
		}
	}

	return wg.Wait
}

func (bh *BackendHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	eventsDispatched := 0
	bh.eventWg.Add(len(bh.backends))
	for _, backend := range bh.backends {
		select {
		case <-ctx.Done():
			// Not all backends got the event, should decrement the wg counter
			bh.eventWg.Add(eventsDispatched - len(bh.backends))
			return
		case bh.concurrentEvents <- struct{}{}:
			// Creates a new context for dispatching the event.
			// We create a new one otherwise it uses the request context which is cancelled as soon as this function returns.
			go func(b gostatsd.Backend) {
				timeoutCtx, cancelTimeout := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancelTimeout()
				bh.internalDispatchEvent(timeoutCtx, b, e)
			}(backend)
			eventsDispatched++
		}
	}
}

// WaitForEvents waits for all event-dispatching goroutines to finish.
func (bh *BackendHandler) WaitForEvents() {
	bh.eventWg.Wait()
}

func (bh *BackendHandler) internalDispatchEvent(ctx context.Context, backend gostatsd.Backend, e *gostatsd.Event) {
	defer bh.eventWg.Done()
	defer func() {
		<-bh.concurrentEvents
	}()
	if err := backend.SendEvent(ctx, e); err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		logrus.Errorf("Sending event to backend failed: %v", err)
	}
}
