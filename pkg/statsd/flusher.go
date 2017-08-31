package statsd

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/statser"

	log "github.com/sirupsen/logrus"
)

// MetricFlusher periodically flushes metrics from all Aggregators to Senders.
type MetricFlusher struct {
	// Counter fields below must be read/written only using atomic instructions.
	// 64-bit fields must be the first fields in the struct to guarantee proper memory alignment.
	// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	lastFlush      int64 // Last time the metrics where aggregated. Unix timestamp in nsec.
	lastFlushError int64 // Time of the last flush error. Unix timestamp in nsec.

	flushInterval time.Duration // How often to flush metrics to the sender
	dispatcher    Dispatcher
	handler       Handler
	backends      []gostatsd.Backend
	selfIP        gostatsd.IP
	hostname      string
	statser       statser.Statser
}

// NewMetricFlusher creates a new MetricFlusher with provided configuration.
func NewMetricFlusher(flushInterval time.Duration, dispatcher Dispatcher, handler Handler, backends []gostatsd.Backend, selfIP gostatsd.IP, hostname string, statser statser.Statser) *MetricFlusher {
	return &MetricFlusher{
		flushInterval: flushInterval,
		dispatcher:    dispatcher,
		handler:       handler,
		backends:      backends,
		selfIP:        selfIP,
		hostname:      hostname,
		statser:       statser,
	}
}

// Run runs the MetricFlusher.
func (f *MetricFlusher) Run(ctx context.Context) {
	flushTicker := time.NewTicker(f.flushInterval)
	defer flushTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-flushTicker.C: // Time to flush to the backends
			f.flushData(ctx)
		}
	}
}

func (f *MetricFlusher) flushData(ctx context.Context) {
	var sendWg sync.WaitGroup
	processWait := f.dispatcher.Process(ctx, func(workerId uint16, aggr Aggregator) {
		aggr.Flush(f.flushInterval)
		aggr.Process(func(m *gostatsd.MetricMap) {
			f.sendMetricsAsync(ctx, &sendWg, m)
		})
		aggr.Reset()
	})
	processWait() // Wait for all workers to execute function
	sendWg.Wait() // Wait for all backends to finish sending
}

func (f *MetricFlusher) sendMetricsAsync(ctx context.Context, wg *sync.WaitGroup, m *gostatsd.MetricMap) {
	wg.Add(len(f.backends))
	for _, backend := range f.backends {
		backend.SendMetricsAsync(ctx, m, func(errs []error) {
			defer wg.Done()
			f.handleSendResult(errs)
		})
	}
}

func (f *MetricFlusher) handleSendResult(flushResults []error) {
	timestampPointer := &f.lastFlush
	for _, err := range flushResults {
		if err != nil {
			timestampPointer = &f.lastFlushError
			if err != context.DeadlineExceeded && err != context.Canceled {
				log.Errorf("Sending metrics to backend failed: %v", err)
			}
		}
	}
	atomic.StoreInt64(timestampPointer, time.Now().UnixNano())
}
