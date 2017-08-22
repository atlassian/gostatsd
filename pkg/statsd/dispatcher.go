package statsd

import (
	"context"
	"fmt"
	"hash/adler32"
	"sync"

	"github.com/ash2k/stager/wait"
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

// Run runs the MetricDispatcher.
func (d *MetricDispatcher) Run(ctx context.Context) {
	var wg wait.Group
	defer func() {
		for _, worker := range d.workers {
			close(worker.metricsQueue) // Close channel to terminate worker
		}
		wg.Wait() // Wait for all workers to finish
	}()
	for _, worker := range d.workers {
		wg.Start(worker.work)
	}

	// Work until asked to stop
	<-ctx.Done()
}

func (d *MetricDispatcher) RunMetrics(ctx context.Context, statser statser.Statser) {
	var wg wait.Group
	defer wg.Wait()
	for _, worker := range d.workers {
		worker := worker
		wg.Start(func() {
			worker.runMetrics(ctx, statser)
		})
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
