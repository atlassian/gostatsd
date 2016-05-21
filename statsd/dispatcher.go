package statsd

import (
	"hash/adler32"
	"sync"
	"time"

	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

// Dispatcher is responsible for managing Aggregators' lifecycle and dispatching metrics among them.
type Dispatcher interface {
	Run(context.Context) error
	DispatchMetric(context.Context, *types.Metric) error
	Flush(context.Context) <-chan *types.MetricMap
	Process(context.Context, ProcessFunc) *sync.WaitGroup
}

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

type flushCommand struct {
	ctx    context.Context
	result chan<- *types.MetricMap
	wg     sync.WaitGroup
}

type processCommand struct {
	f  ProcessFunc
	wg sync.WaitGroup
}

type worker struct {
	aggr         Aggregator
	flushChan    chan *flushCommand
	metricsQueue chan *types.Metric
	processChan  chan *processCommand
}

type dispatcher struct {
	numWorkers int
	workers    map[uint16]worker
}

// NewDispatcher creates a new Dispatcher with provided configuration.
func NewDispatcher(numWorkers int, perWorkerBufferSize int, af AggregatorFactory) Dispatcher {
	workers := make(map[uint16]worker, numWorkers)

	n := uint16(numWorkers)

	for i := uint16(0); i < n; i++ {
		workers[i] = worker{
			aggr:         af.Create(),
			flushChan:    make(chan *flushCommand),
			metricsQueue: make(chan *types.Metric, perWorkerBufferSize),
			processChan:  make(chan *processCommand),
		}
	}
	return &dispatcher{
		numWorkers: numWorkers,
		workers:    workers,
	}
}

// Run runs the Dispatcher.
func (d *dispatcher) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(d.numWorkers)
	for _, worker := range d.workers {
		w := worker // Make a copy of the loop variable! https://github.com/golang/go/wiki/CommonMistakes
		go w.work(&wg)
	}
	defer func() {
		for _, worker := range d.workers {
			close(worker.metricsQueue) // Close channel to terminate worker
		}
		wg.Wait() // Wait for all workers to finish
	}()

	// Work until asked to stop
	<-ctx.Done()
	return ctx.Err()
}

// DispatchMetric dispatches metric to a corresponding Aggregator.
func (d *dispatcher) DispatchMetric(ctx context.Context, m *types.Metric) error {
	hash := adler32.Checksum([]byte(m.Name))
	w := d.workers[uint16(hash%uint32(d.numWorkers))]
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.metricsQueue <- m:
		return nil
	}
}

// Flush calls Flush on all managed Aggregators and returns results.
func (d *dispatcher) Flush(ctx context.Context) <-chan *types.MetricMap {
	results := make(chan *types.MetricMap, d.numWorkers) // Enough capacity not to block workers
	cmd := &flushCommand{
		ctx:    ctx,
		result: results,
	}
	cmd.wg.Add(len(d.workers))
	flushesSent := 0
loop:
	for _, worker := range d.workers {
		select {
		case <-ctx.Done():
			cmd.wg.Add(flushesSent - len(d.workers)) // Not all flushes have been sent, should decrement the WG counter.
			break loop
		case worker.flushChan <- cmd:
			flushesSent++
		}
	}
	go func() {
		// Wait until all the workers finish to ensure we close the channel only after we know nobody is going to send to it
		cmd.wg.Wait()
		close(results) // Signal consumer there are no more flush results
	}()
	return results
}

// Process concurrently executes provided function in goroutines that own Aggregators.
// ProcessFunc function may be executed zero or up to numWorkers times. It is executed
// less than numWorkers times if the context signals "done".
func (d *dispatcher) Process(ctx context.Context, f ProcessFunc) *sync.WaitGroup {
	cmd := &processCommand{
		f: f,
	}
	cmd.wg.Add(len(d.workers))
	cmdSent := 0
loop:
	for _, worker := range d.workers {
		select {
		case <-ctx.Done():
			cmd.wg.Add(cmdSent - len(d.workers)) // Not all commands have been sent, should decrement the WG counter.
			break loop
		case worker.processChan <- cmd:
			cmdSent++
		}
	}

	return &cmd.wg
}

func (w *worker) work(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case metric, ok := <-w.metricsQueue:
			if !ok {
				log.Info("Worker metrics queue was closed, exiting")
				return
			}
			w.aggr.Receive(metric, time.Now())
		case cmd := <-w.flushChan:
			w.executeFlush(cmd)
		case cmd := <-w.processChan:
			w.executeProcess(cmd)
		}
	}
}

func (w *worker) executeFlush(cmd *flushCommand) {
	defer cmd.wg.Done()              // Done with the flush command
	result := w.aggr.Flush(time.Now) // pass func for stubbing
	w.aggr.Reset(time.Now())
	select {
	case <-cmd.ctx.Done():
	case cmd.result <- result:
	}
}

func (w *worker) executeProcess(cmd *processCommand) {
	defer cmd.wg.Done() // Done with the process command
	w.aggr.Process(cmd.f)
}
