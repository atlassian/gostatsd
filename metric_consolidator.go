package gostatsd

import (
	"context"
	"time"

	"github.com/tilinna/clock"
)

// MetricConsolidator will consolidate metrics randomly in to a slice of MetricMaps, and send the slice to the provided
// channel.  Run can be started in a long running goroutine to perform flushing, or Flush can be called externally.
//
// Used to consolidate metrics such as:
// - counter[name=x, value=1]
// - counter[name=x, value=1]
// - counter[name=x, value=1]
// - counter[name=x, value=1]
// - counter[name=x, value=1]
//
// in to:
// - counter[name=x, value=5]
//
// Similar consolidation is performed for other metric types.
type MetricConsolidator struct {
	maps          chan *MetricMap
	sink          chan<- []*MetricMap
	flushInterval time.Duration
}

func NewMetricConsolidator(spots int, flushInterval time.Duration, sink chan<- []*MetricMap) *MetricConsolidator {
	mc := &MetricConsolidator{}
	mc.maps = make(chan *MetricMap, spots)
	for i := 0; i < spots; i++ {
		mc.maps <- NewMetricMap()
	}
	mc.flushInterval = flushInterval
	mc.sink = sink
	return mc
}

func (mc *MetricConsolidator) Run(ctx context.Context) {
	t := clock.NewTicker(ctx, mc.flushInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			mc.Flush(ctx)
		}
	}
}

// Flush will collect all the MetricMaps in to a slice, send them to the channel provided, then
// create new MetricMaps for new metrics to land in.  Not thread-safe.
func (mc *MetricConsolidator) Flush(ctx context.Context) {
	var mms []*MetricMap
	for i := 0; i < cap(mc.maps); i++ {
		select {
		case mm := <-mc.maps:
			mms = append(mms, mm)
		case <-ctx.Done():
			// Put everything back, so we're consistent, just in case.  No need to check for termination,
			// because we know it will fit exactly.
			for _, mm := range mms {
				mc.maps <- mm
			}
			return
		}
	}

	// Send the collected data to the sink before putting new maps in place.  This allows back-pressure
	// to propagate through the system, if the sink can't keep up.
	select {
	case mc.sink <- mms:
	case <-ctx.Done():
	}

	for i := 0; i < cap(mc.maps); i++ {
		mc.maps <- NewMetricMap()
	}
}

// ReceiveMetrics will push a slice of Metrics in to one of the MetricMaps
func (mc *MetricConsolidator) ReceiveMetrics(metrics []*Metric) {
	mmTo := <-mc.maps
	for _, m := range metrics {
		mmTo.Receive(m)
	}
	mc.maps <- mmTo
}

// ReceiveMetricMap will merge a MetricMap in to one of the MetricMaps
func (mc *MetricConsolidator) ReceiveMetricMap(mm *MetricMap) {
	mmTo := <-mc.maps
	mmTo.Merge(mm)
	mc.maps <- mmTo
}
