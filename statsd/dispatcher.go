package statsd

import (
	"hash/adler32"
	"sync"
	"time"

	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

// DispatcherProcessFunc is a function that gets executed by Dispatcher for each Aggregator, passing it into the function.
type DispatcherProcessFunc func(uint16, Aggregator)

// Dispatcher is responsible for managing Aggregators' lifecycle and dispatching metrics among them.
type Dispatcher interface {
	Run(context.Context) error
	DispatchMetric(context.Context, *types.Metric) error
	Process(context.Context, DispatcherProcessFunc) *sync.WaitGroup
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

type processCommand struct {
	f  DispatcherProcessFunc
	wg sync.WaitGroup
}

type worker struct {
	aggr         Aggregator
	metricsQueue chan *types.Metric
	processChan  chan *processCommand
	id           uint16
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
			metricsQueue: make(chan *types.Metric, perWorkerBufferSize),
			processChan:  make(chan *processCommand),
			id:           i,
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

// Process concurrently executes provided function in goroutines that own Aggregators.
// DispatcherProcessFunc function may be executed zero or up to numWorkers times. It is executed
// less than numWorkers times if the context signals "done".
func (d *dispatcher) Process(ctx context.Context, f DispatcherProcessFunc) *sync.WaitGroup {
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
		case cmd := <-w.processChan:
			w.executeProcess(cmd)
		}
	}
}

func (w *worker) executeProcess(cmd *processCommand) {
	defer cmd.wg.Done() // Done with the process command
	cmd.f(w.id, w.aggr)
}
