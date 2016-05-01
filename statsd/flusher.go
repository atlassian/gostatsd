package statsd

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	backendTypes "github.com/atlassian/gostatsd/backend/types"
	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

// FlusherStats holds statistics about a Flusher.
type FlusherStats struct {
	LastFlush      time.Time // Last time the metrics where aggregated
	LastFlushError time.Time // Time of the last flush error
}

// Flusher periodically flushes metrics from all Aggregators to Senders.
type Flusher interface {
	Run(context.Context) error
	GetStats() FlusherStats
}

type flusher struct {
	// Counter fields below must be read/written only using atomic instructions.
	// 64-bit fields must be the first fields in the struct to guarantee proper memory alignment.
	// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	lastFlush      int64 // Last time the metrics where aggregated. Unix timestamp in nsec.
	lastFlushError int64 // Time of the last flush error. Unix timestamp in nsec.

	flushInterval time.Duration // How often to flush metrics to the sender
	dispatcher    Dispatcher
	receiver      Receiver
	defaultTags   string
	senders       []backendTypes.MetricSender

	// Sent statistics for Receiver. Keep sent values to calculate diff.
	sentBadLines        uint64
	sentPacketsReceived uint64
	sentMetricsReceived uint64
}

// NewFlusher creates a new Flusher with provided configuration.
func NewFlusher(flushInterval time.Duration, dispatcher Dispatcher, receiver Receiver, defaultTags []string, senders []backendTypes.MetricSender) Flusher {
	return &flusher{
		flushInterval: flushInterval,
		dispatcher:    dispatcher,
		receiver:      receiver,
		defaultTags:   strings.Join(defaultTags, ","),
		senders:       senders,
	}
}

// Run runs the Flusher.
func (f *flusher) Run(ctx context.Context) error {
	flushTimer := time.NewTimer(f.flushInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-flushTimer.C: // Time to flush to the backends
			f.flushData(ctx)
			flushTimer = time.NewTimer(f.flushInterval)
		}
	}
}

// GetStats returns Flusher statistics.
func (f *flusher) GetStats() FlusherStats {
	return FlusherStats{
		time.Unix(0, atomic.LoadInt64(&f.lastFlush)),
		time.Unix(0, atomic.LoadInt64(&f.lastFlushError)),
	}
}

func (f *flusher) flushData(ctx context.Context) {
	results := f.dispatcher.Flush(ctx)
	var totalStats uint32
	for {
		select {
		case <-ctx.Done():
			return
		case result, ok := <-results:
			if !ok {
				f.sendFlushedData(ctx, f.internalStats(totalStats))
				return
			}
			totalStats += result.NumStats
			f.sendFlushedData(ctx, result)
		}
	}
}

func (f *flusher) sendFlushedData(ctx context.Context, metrics *types.MetricMap) {
	var wg sync.WaitGroup
	wg.Add(len(f.senders))
	for _, sender := range f.senders {
		go func(s backendTypes.MetricSender) {
			defer wg.Done()
			log.Debugf("Sending metrics to backend %s", s.BackendName())
			//TODO pass ctx
			f.handleSendResult(s.SendMetrics(*metrics))
		}(sender)
	}
	wg.Wait()
}

func (f *flusher) handleSendResult(flushResult error) {
	timestamp := time.Now().UnixNano()
	if flushResult != nil {
		log.Errorf("Sending metrics to backend failed: %v", flushResult)
		atomic.StoreInt64(&f.lastFlushError, timestamp)
	} else {
		atomic.StoreInt64(&f.lastFlush, timestamp)
	}
}

func (f *flusher) internalStats(totalStats uint32) *types.MetricMap {
	receiverStats := f.receiver.GetStats()
	now := time.Now()
	c := make(types.Counters, 4)
	f.addCounter(c, "bad_lines_seen", now, int64(receiverStats.BadLines-f.sentBadLines))
	f.addCounter(c, "metrics_received", now, int64(receiverStats.MetricsReceived-f.sentMetricsReceived))
	f.addCounter(c, "packets_received", now, int64(receiverStats.PacketsReceived-f.sentPacketsReceived))
	f.addCounter(c, "total_num_stats", now, int64(totalStats))

	log.Infof("total_num_stats: %d", totalStats)

	f.sentBadLines = receiverStats.BadLines
	f.sentMetricsReceived = receiverStats.MetricsReceived
	f.sentPacketsReceived = receiverStats.PacketsReceived

	return &types.MetricMap{
		NumStats:       4,
		ProcessingTime: time.Duration(0),
		FlushInterval:  f.flushInterval,
		Counters:       c,
	}
}

func (f *flusher) addCounter(c types.Counters, name string, timestamp time.Time, value int64) {
	counter := types.NewCounter(timestamp, f.flushInterval, value)
	counter.PerSecond = float64(counter.Value) / (float64(f.flushInterval) / float64(time.Second))

	elem := make(map[string]types.Counter, 1)
	elem[f.defaultTags] = counter

	c[internalStatName(name)] = elem
}
