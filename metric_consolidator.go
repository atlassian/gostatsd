package gostatsd

import (
	"context"
	"time"

	"github.com/tilinna/clock"
)

// MetricConsolidator will consolidate metrics randomly in to a slice of MetricMaps, and send the slice to the provided
// channel.  Run can be started in a long running goroutine to perform flushing, or Flush can be called externally.
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

func (mc *MetricConsolidator) Flush(ctx context.Context) {
	var mms []*MetricMap
	for i := 0; i < cap(mc.maps); i++ {
		mm := mc.getMap(ctx)
		if mm == nil {
			// Put everything back, so we're consistent, just in case.  No need to check for termination,
			// because we know it will fit exactly.
			for _, mm := range mms {
				mc.maps <- mm
			}
			return
		}
		mms = append(mms, mm)
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

// ReceiveMetric will push a Metric in to one of the MetricMaps
func (mc *MetricConsolidator) ReceiveMetric(ctx context.Context, m *Metric) {
	mm := mc.getMap(ctx)
	if mm == nil {
		return
	}
	mm.Receive(m, clock.Now(ctx))
	mc.maps <- mm
}

// ReceiveMetricMap will merge a MetricMap in to one of the MetricMaps
func (mc *MetricConsolidator) ReceiveMetricMap(ctx context.Context, mm *MetricMap) {
	mmTo := mc.getMap(ctx)
	if mmTo == nil {
		return
	}
	mmTo.Merge(mm)
	mc.maps <- mmTo
}

// getMap will return a MetricMap or nil if the context is cancelled.  There is no putMap, as that will never block.
func (mc *MetricConsolidator) getMap(ctx context.Context) *MetricMap {
	select {
	case <-ctx.Done():
		return nil
	case mm := <- mc.maps:
		return mm
	}
}
