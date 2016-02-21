package statsd

import (
	"sort"
	"sync"
	"time"

	"github.com/jtblin/gostatsd/backend"
	"github.com/jtblin/gostatsd/types"

	log "github.com/Sirupsen/logrus"
)

// metricAggregatorStats is a bookkeeping structure for statistics about a MetricAggregator
type metricAggregatorStats struct {
	BadLines       int
	LastMessage    time.Time
	LastFlush      time.Time
	LastFlushError time.Time
}

// MetricAggregator is an object that aggregates statsd metrics.
// The function NewMetricAggregator should be used to create the objects.
//
// Incoming metrics should be sent to the MetricChan channel.
type MetricAggregator struct {
	sync.Mutex
	MetricChan    chan types.Metric      // Channel on which metrics are received
	FlushInterval time.Duration          // How often to flush metrics to the sender
	Senders       []backend.MetricSender // The sender to which metrics are flushed
	Stats         metricAggregatorStats
	Counters      types.MetricMap
	Gauges        types.MetricMap
	Timers        types.MetricListMap
}

// NewMetricAggregator creates a new MetricAggregator object
func NewMetricAggregator(senders []backend.MetricSender, flushInterval time.Duration) MetricAggregator {
	a := MetricAggregator{}
	a.FlushInterval = flushInterval
	a.Senders = senders
	a.MetricChan = make(chan types.Metric)
	a.Counters = make(types.MetricMap)
	a.Gauges = make(types.MetricMap)
	a.Timers = make(types.MetricListMap)
	return a
}

// flush prepares the contents of a MetricAggregator for sending via the Sender
func (a *MetricAggregator) flush() (metrics types.MetricMap) {
	defer a.Unlock()
	a.Lock()

	metrics = make(types.MetricMap)
	numStats := 0

	for k, v := range a.Counters {
		perSecond := v / a.FlushInterval.Seconds()
		metrics["stats."+k] = perSecond
		metrics["stats_counts."+k] = v
		numStats += 1
	}

	for k, v := range a.Gauges {
		metrics["stats.gauges."+k] = v
		numStats += 1
	}

	for k, v := range a.Timers {
		if count := len(v); count > 0 {
			sort.Float64s(v)
			min := v[0]
			max := v[count-1]

			metrics["stats.timers."+k+".lower"] = min
			metrics["stats.timers."+k+".upper"] = max
			metrics["stats.timers."+k+".count"] = float64(count)
			numStats += 1
		}
	}
	metrics["statsd.numStats"] = float64(numStats)
	return metrics
}

// Reset clears the contents of a MetricAggregator
func (a *MetricAggregator) Reset() {
	log.Debug("Reset metrics")
	defer a.Unlock()
	a.Lock()

	for k := range a.Counters {
		a.Counters[k] = 0
	}

	for k := range a.Timers {
		a.Timers[k] = []float64{}
	}

	// No reset for gauges, they keep the last value
}

// receiveMetric is called for each incoming metric on MetricChan
func (a *MetricAggregator) receiveMetric(m types.Metric) {
	defer a.Unlock()
	a.Lock()

	switch m.Type {
	case types.COUNTER:
		v, ok := a.Counters[m.Bucket]
		if ok {
			a.Counters[m.Bucket] = v + m.Value
		} else {
			a.Counters[m.Bucket] = m.Value
		}
	case types.GAUGE:
		a.Gauges[m.Bucket] = m.Value
	case types.TIMER:
		v, ok := a.Timers[m.Bucket]
		if ok {
			v = append(v, m.Value)
			a.Timers[m.Bucket] = v
		} else {
			a.Timers[m.Bucket] = []float64{m.Value}
		}
	case types.ERROR:
		a.Stats.BadLines += 1
	}
	a.Stats.LastMessage = time.Now()
}

// Aggregate starts the MetricAggregator so it begins consuming metrics from MetricChan
// and flushing them periodically via its Sender
func (a *MetricAggregator) Aggregate() {
	flushChan := make(chan error)
	flushTimer := time.NewTimer(a.FlushInterval)

	for {
		select {
		case metric := <-a.MetricChan: // Incoming metrics
			a.receiveMetric(metric)
		case <-flushTimer.C: // Time to flush to graphite
			flushed := a.flush()
			for _, sender := range a.Senders {
				s := sender
				go func() {
					log.Debugf("Send metrics to backend %s", s.Name())
					flushChan <- s.SendMetrics(flushed)
				}()
			}
			a.Reset()
			flushTimer = time.NewTimer(a.FlushInterval)
		case flushResult := <-flushChan:
			a.Lock()

			if flushResult != nil {
				log.Printf("Sending metrics to backends failed: %s", flushResult)
				a.Stats.LastFlushError = time.Now()
			} else {
				a.Stats.LastFlush = time.Now()
			}
			a.Unlock()
		}
	}

}
