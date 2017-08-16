package statsd

import (
	"context"
	"fmt"
	"hash/adler32"
	"sync"

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

// MetricDispatcher dispatches incoming metrics to corresponding aggregators.
type MetricDispatcher struct {
	numWorkers int
	workers    map[uint16]*worker
	lock       sync.RWMutex
	running    bool
}

// NewMetricDispatcher creates a new NewMetricDispatcher with provided configuration.
func NewMetricDispatcher(numWorkers int, perWorkerBufferSize int, af AggregatorFactory) *MetricDispatcher {
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
	return &MetricDispatcher{
		numWorkers: numWorkers,
		workers:    workers,
	}
}

// RunAsync runs the MetricDispatcher in a goroutine
func (d *MetricDispatcher) RunAsync(ctx context.Context, done gostatsd.Done) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.running = true
	go func() {
		defer done()

		var wg sync.WaitGroup
		wg.Add(d.numWorkers)
		for _, worker := range d.workers {
			w := worker // Make a copy of the loop variable! https://github.com/golang/go/wiki/CommonMistakes
			go w.work(wg.Done)
		}

		defer func() {
			// Once this is indicated as not running, DispatchMetric will no longer
			// try to send to the channel, so it is safe to close.
			d.lock.Lock()
			d.running = false
			d.lock.Unlock()

			for _, worker := range d.workers {
				close(worker.metricsQueue) // Close channel to terminate worker
			}
			wg.Wait() // Wait for all workers to finish
		}()

		// Work until asked to stop
		<-ctx.Done()
	}()
}

func (d *MetricDispatcher) runMetrics(ctx context.Context, statser statser.Statser) {
	for _, worker := range d.workers {
		worker.runMetrics(ctx, statser)
	}
	d.Process(ctx, func(aggrId uint16, aggr Aggregator) {
		tag := fmt.Sprintf("aggregator_id:%d", aggrId)
		aggr.TrackMetrics(statser.WithTags(gostatsd.Tags{tag}))
	})
}

// DispatchMetric dispatches metric to a corresponding Aggregator.
func (d *MetricDispatcher) DispatchMetric(ctx context.Context, m *gostatsd.Metric) error {
	hash := adler32.Checksum([]byte(m.Name))
	w := d.workers[uint16(hash%uint32(d.numWorkers))]

	// Protects both the read of d.running, and that the channel remains open
	d.lock.RLock()
	defer d.lock.RUnlock()
	if !d.running {
		return nil
	}

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
func (d *MetricDispatcher) Process(ctx context.Context, f DispatcherProcessFunc) gostatsd.Wait {
	var wg sync.WaitGroup
	cmd := &processCommand{
		f:    f,
		done: wg.Done,
	}
	wg.Add(d.numWorkers)
	cmdSent := 0
loop:
	for _, worker := range d.workers {
		select {
		case <-ctx.Done():
			wg.Add(cmdSent - d.numWorkers) // Not all commands have been sent, should decrement the WG counter.
			break loop
		case worker.processChan <- cmd:
			cmdSent++
		}
	}

	return wg.Wait
}
