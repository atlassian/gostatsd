package statsd

import (
	"sync"
	"time"

	"github.com/atlassian/gostatsd"
)

type processCommand struct {
	f  DispatcherProcessFunc
	wg sync.WaitGroup
}

type worker struct {
	aggr         Aggregator
	metricsQueue chan *gostatsd.Metric
	processChan  chan *processCommand
	id           uint16
}

func (w *worker) work(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case metric, ok := <-w.metricsQueue:
			if !ok {
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
