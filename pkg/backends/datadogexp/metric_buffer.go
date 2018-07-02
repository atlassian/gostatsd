package datadogexp

import (
	"context"

	"github.com/sirupsen/logrus"

	"github.com/atlassian/gostatsd"
)

// metricBuffer will process aggregated data in to a single map, accumulating multiple points
// for a timeseries.  If too many points are accumulated, it will attempt to push data to the
// upstream buffer, blocking as required.
type metricBuffer struct {
	logger logrus.FieldLogger

	disabledSubTypes *gostatsd.TimerSubtypes
	timeSeriesBuffer ddMetricMap
	flushIntervalSec float64
	capacity         int

	// metricsQueue is the channel via which batches of aggregated data is submitted to the metricBuffer.  The
	// chan must be as deep as there is number of aggregators sending data, or the aggregators may block early.
	// This is where backpressure enters the handler pipeline.  The batchMessage done function will be executed
	// after being processed.
	metricsQueue <-chan *batchMessage

	// bufferRequested is a channel where an upstream component can notify the metricBuffer that it is idle.
	// The metricBuffer may ignore a request for data.  It is the responsibility of the upstream to request
	// data on a regular interval, even if none is provided.
	bufferRequested <-chan struct{}

	// bufferReady is where a metricBuffer will deliver a ddMetricMap for further processing.  This is expected
	// to occur after a buffer is requested via bufferRequested, or when the metricBuffer has reached a threshold
	// of too much data.  If the upstream handler is busy, this is where metricBuffer gets back pressure, which
	// propagates through the system as reading of the metricsQueue will halt.
	bufferReady chan<- ddMetricMap
}

// Run pulls data from the raw queue and accumulates it
func (mb *metricBuffer) Run(ctx context.Context) {
	mb.logger.Info("Starting")
	defer mb.logger.Info("Terminating")

	mb.timeSeriesBuffer = ddMetricMap{}

	for {
		select {
		case <-ctx.Done():
			return
		case mm := <-mb.metricsQueue:
			if mb.processMetricBuffer(ctx, mm) {
				mb.logger.Info("Forcing flush")
				mb.flush(ctx)
			}
		case <-mb.bufferRequested:
			mb.logger.Info("Requested flush")
			mb.flush(ctx)
		}
	}
}

func (mb *metricBuffer) processMetricBuffer(ctx context.Context, mm *batchMessage) bool {
	timestamp := float64(mm.flushTime.Unix())

	counters, forceFlushC := mb.processCounters(mm, timestamp)
	timers, forceFlushT := mb.processTimers(mm, timestamp)
	gauges, forceFlushG := mb.processGauges(mm, timestamp)
	sets, forceFlushS := mb.processSets(mm, timestamp)

	mb.logger.WithFields(logrus.Fields{
		"counters": counters,
		"timers":   timers,
		"gauges":   gauges,
		"sets":     sets,
	}).Info("Processed metrics")

	// Signal done
	mm.done(nil)

	return forceFlushC || forceFlushT || forceFlushG || forceFlushS
}

func (mb *metricBuffer) processCounters(mm *batchMessage, timestamp float64) (int, bool) {
	counters := 0
	forceFlush := false

	mm.metrics.Counters.Each(func(metricName, tagsKey string, c gostatsd.Counter) {
		counters++
		if mb.addMetric(rate, ddPoint{timestamp, c.PerSecond}, c.Hostname, c.Tags, tagsKey, metricName) {
			forceFlush = true
		}
		if mb.addMetric(gauge, ddPoint{timestamp, float64(c.Value)}, c.Hostname, c.Tags, tagsKey, metricName+".count") {
			forceFlush = true
		}
	})

	return counters, forceFlush
}

func (mb *metricBuffer) processTimers(mm *batchMessage, timestamp float64) (int, bool) {
	timers := 0
	forceFlush := false

	disabled := mb.disabledSubTypes

	mm.metrics.Timers.Each(func(metricName, tagsKey string, t gostatsd.Timer) {
		timers++
		if !disabled.Lower {
			if mb.addMetric(gauge, ddPoint{timestamp, t.Min}, t.Hostname, t.Tags, tagsKey, metricName+".lower") {
				forceFlush = true
			}
		}
		if !disabled.Upper {
			if mb.addMetric(gauge, ddPoint{timestamp, t.Max}, t.Hostname, t.Tags, tagsKey, metricName+".upper") {
				forceFlush = true
			}
		}
		if !disabled.Count {
			if mb.addMetric(gauge, ddPoint{timestamp, float64(t.Count)}, t.Hostname, t.Tags, tagsKey, metricName+".count") {
				forceFlush = true
			}
		}
		if !disabled.CountPerSecond {
			if mb.addMetric(rate, ddPoint{timestamp, t.PerSecond}, t.Hostname, t.Tags, tagsKey, metricName+".count_ps") {
				forceFlush = true
			}
		}
		if !disabled.Mean {
			if mb.addMetric(gauge, ddPoint{timestamp, t.Mean}, t.Hostname, t.Tags, tagsKey, metricName+".mean") {
				forceFlush = true
			}
		}
		if !disabled.Median {
			if mb.addMetric(gauge, ddPoint{timestamp, t.Median}, t.Hostname, t.Tags, tagsKey, metricName+".median") {
				forceFlush = true
			}
		}
		if !disabled.StdDev {
			if mb.addMetric(gauge, ddPoint{timestamp, t.StdDev}, t.Hostname, t.Tags, tagsKey, metricName+".std") {
				forceFlush = true
			}
		}
		if !disabled.Sum {
			if mb.addMetric(gauge, ddPoint{timestamp, t.Sum}, t.Hostname, t.Tags, tagsKey, metricName+".sum") {
				forceFlush = true
			}
		}
		if !disabled.SumSquares {
			if mb.addMetric(gauge, ddPoint{timestamp, t.SumSquares}, t.Hostname, t.Tags, tagsKey, metricName+".sum_squares") {
				forceFlush = true
			}
		}
		for _, pct := range t.Percentiles {
			if mb.addMetric(gauge, ddPoint{timestamp, pct.Float}, t.Hostname, t.Tags, tagsKey, metricName+"."+pct.Str) {
				forceFlush = true
			}
		}
	})

	return timers, forceFlush
}

func (mb *metricBuffer) processGauges(mm *batchMessage, timestamp float64) (int, bool) {
	gauges := 0
	forceFlush := false

	mm.metrics.Gauges.Each(func(metricName, tagsKey string, g gostatsd.Gauge) {
		gauges++
		if mb.addMetric(gauge, ddPoint{timestamp, g.Value}, g.Hostname, g.Tags, tagsKey, metricName) {
			forceFlush = true
		}
	})

	return gauges, forceFlush
}

func (mb *metricBuffer) processSets(mm *batchMessage, timestamp float64) (int, bool) {
	sets := 0
	forceFlush := false

	mm.metrics.Sets.Each(func(metricName, tagsKey string, s gostatsd.Set) {
		sets++
		if mb.addMetric(gauge, ddPoint{timestamp, float64(len(s.Values))}, s.Hostname, s.Tags, tagsKey, metricName) {
			forceFlush = true
		}
	})

	return sets, forceFlush
}

func (mb *metricBuffer) flush(ctx context.Context) {
	if len(mb.timeSeriesBuffer) == 0 {
		return
	}

	select {
	case <-ctx.Done():
		return
	case mb.bufferReady <- mb.timeSeriesBuffer:
		mb.timeSeriesBuffer = ddMetricMap{}
		mb.logger.Info("Data flushed")
	}
}

func (mb *metricBuffer) newMetric(hostname, metricName string, tags gostatsd.Tags, metricType metricType) *ddMetric {
	return &ddMetric{
		Host:     hostname,
		Interval: mb.flushIntervalSec,
		Metric:   metricName,
		Points:   make([]ddPoint, 0, mb.capacity),
		Tags:     tags,
		Type:     metricType,
	}
}

func (mb *metricBuffer) addMetric(metricType metricType, point ddPoint, hostname string, tags gostatsd.Tags, tagsKey, metricName string) bool {
	var metric *ddMetric

	if tagMap, ok := mb.timeSeriesBuffer[metricName]; !ok {
		// Name not found
		metric = mb.newMetric(hostname, metricName, tags, metricType)
		mb.timeSeriesBuffer[metricName] = map[string]*ddMetric{
			tagsKey: metric,
		}
	} else if metric, ok = tagMap[tagsKey]; !ok { // Sets metric if it succeeds
		// Tagset not found
		metric = mb.newMetric(hostname, metricName, tags, metricType)
		tagMap[tagsKey] = metric
	}

	metric.Points = append(metric.Points, point)
	return len(metric.Points) >= mb.capacity
}
