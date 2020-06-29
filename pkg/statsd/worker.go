package statsd

import (
	"context"
	"fmt"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"
)

type processCommand struct {
	f    DispatcherProcessFunc
	done func()
}

type worker struct {
	aggr           Aggregator
	metricsQueue   chan []*gostatsd.Metric // Batches
	metricMapQueue chan *gostatsd.MetricMap
	processChan    chan *processCommand
	id             int
}

func (w *worker) work() {
	for {
		select {
		case metrics, ok := <-w.metricsQueue:
			if !ok {
				return
			}
			w.aggr.Receive(metrics...)
		case mm, ok := <-w.metricMapQueue:
			if !ok {
				return
			}
			w.aggr.ReceiveMap(mm)
		case cmd := <-w.processChan:
			w.executeProcess(cmd)
		}
	}
}

func (w *worker) executeProcess(cmd *processCommand) {
	defer cmd.done() // Done with the process command
	cmd.f(w.id, w.aggr)
}

func (w *worker) RunMetrics(ctx context.Context, statser stats.Statser) {
	go stats.NewChannelStatsWatcher(
		statser,
		"dispatch_aggregator_batch",
		gostatsd.Tags{fmt.Sprintf("aggregator_id:%d", w.id)},
		cap(w.metricsQueue),
		func() int { return len(w.metricsQueue) },
		1000*time.Millisecond, // TODO: Make configurable
	).Run(ctx)

	stats.NewChannelStatsWatcher(
		statser,
		"dispatch_aggregator_map",
		gostatsd.Tags{fmt.Sprintf("aggregator_id:%d", w.id)},
		cap(w.metricMapQueue),
		func() int { return len(w.metricMapQueue) },
		1000*time.Millisecond,
	).Run(ctx)
}
