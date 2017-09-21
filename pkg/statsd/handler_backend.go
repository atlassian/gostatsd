package statsd

import (
	"context"
	"fmt"
	"hash/adler32"
	"sync"

	"github.com/ash2k/stager/wait"
	"github.com/sirupsen/logrus"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/statser"
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
	workers    map[uint16]*worker
}

// NewBackendHandler initialises a new Handler which sends metrics and events to all backends
func NewBackendHandler(backends []gostatsd.Backend, maxConcurrentEvents uint, numWorkers int, perWorkerBufferSize int, af AggregatorFactory) *BackendHandler {
	workers := make(map[uint16]*worker, numWorkers)

	n := uint16(numWorkers)

	for i := uint16(0); i < n; i++ {
		workers[i] = &worker{
			aggr:         af.Create(),
			metricsQueue: make(chan *gostatsd.Metric, perWorkerBufferSize),
			processChan:  make(chan *processCommand),
			id:           i,
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
			close(worker.metricsQueue) // Close channel to terminate worker
		}
		wg.Wait() // Wait for all workers to finish
	}()
	for _, worker := range bh.workers {
		wg.Start(worker.work)
	}

	// Work until asked to stop
	<-ctx.Done()
}

// RunMetrics attaches a Statser to the BackendHandler.  Stops when the context is closed.
func (bh *BackendHandler) RunMetrics(ctx context.Context, statser statser.Statser) {
	var wg wait.Group
	defer wg.Wait()
	for _, worker := range bh.workers {
		worker := worker
		wg.Start(func() {
			worker.runMetrics(ctx, statser)
		})
	}
	bh.Process(ctx, func(aggrId uint16, aggr Aggregator) {
		tag := fmt.Sprintf("aggregator_id:%d", aggrId)
		aggr.TrackMetrics(statser.WithTags(gostatsd.Tags{tag}))
	})
}

// DispatchMetric dispatches metric to a corresponding Aggregator.
func (bh *BackendHandler) DispatchMetric(ctx context.Context, m *gostatsd.Metric) error {
	hash := adler32.Checksum([]byte(m.Name))
	w := bh.workers[uint16(hash%uint32(bh.numWorkers))]
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.metricsQueue <- m:
		return nil
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

func (bh *BackendHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) error {
	eventsDispatched := 0
	bh.eventWg.Add(len(bh.backends))
	for _, backend := range bh.backends {
		select {
		case <-ctx.Done():
			// Not all backends got the event, should decrement the wg counter
			bh.eventWg.Add(eventsDispatched - len(bh.backends))
			return ctx.Err()
		case bh.concurrentEvents <- struct{}{}:
			go bh.dispatchEvent(ctx, backend, e)
			eventsDispatched++
		}
	}
	return nil
}

// WaitForEvents waits for all event-dispatching goroutines to finish.
func (bh *BackendHandler) WaitForEvents() {
	bh.eventWg.Wait()
}

func (bh *BackendHandler) dispatchEvent(ctx context.Context, backend gostatsd.Backend, e *gostatsd.Event) {
	defer bh.eventWg.Done()
	defer func() {
		<-bh.concurrentEvents
	}()
	if err := backend.SendEvent(ctx, e); err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		logrus.Errorf("Sending event to backend failed: %v", err)
	}
}
