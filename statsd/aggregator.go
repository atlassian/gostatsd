package statsd

import (
	"log"
	"sort"
	"sync"
	"time"
)

// metricAggregatorStats is a bookkeeping structure for statistics about a MetricAggregator
type metricAggregatorStats struct {
	BadLines          int
	LastMessage       time.Time
	GraphiteLastFlush time.Time
	GraphiteLastError time.Time
}

// MetricSender is an interface that can be implemented by objects which
// could be connected to a MetricAggregator
type MetricSender interface {
	SendMetrics(MetricMap) error
}

// MetricAggregator is an object that aggregates statsd metrics.
// The function NewMetricAggregator should be used to create the objects.
//
// Incoming metrics should be sent to the MetricChan channel.
type MetricAggregator struct {
	sync.Mutex
	MetricChan    chan Metric   // Channel on which metrics are received
	FlushInterval time.Duration // How often to flush metrics to the sender
	Sender        MetricSender  // The sender to which metrics are flushed
	stats         metricAggregatorStats
	counters      MetricMap
	gauges        MetricMap
	timers        MetricListMap
}

// NewMetricAggregator creates a new MetricAggregator object
func NewMetricAggregator(sender MetricSender, flushInterval time.Duration) (a MetricAggregator) {
	a = MetricAggregator{}
	a.FlushInterval = flushInterval
	a.Sender = sender
	a.MetricChan = make(chan Metric)
	return
}

// flush prepares the contents of a MetricAggregator for sending via the Sender
func (m *MetricAggregator) flush() (metrics MetricMap) {
	metrics = make(MetricMap)
	numStats := 0

	for k, v := range m.counters {
		perSecond := v / m.FlushInterval.Seconds()
		metrics["stats."+k] = perSecond
		metrics["stats_counts."+k] = v
		numStats += 1
	}

	for k, v := range m.gauges {
		metrics["stats.gauges."+k] = v
		numStats += 1
	}

	for k, v := range m.timers {
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
	// Reset counters
	new_counters := make(MetricMap)
	for k := range a.counters {
		new_counters[k] = 0
	}
	a.counters = new_counters

	// Reset timers
	new_timers := make(MetricListMap)
	for k := range a.timers {
		new_timers[k] = []float64{}
	}
	a.timers = new_timers

	// Keep values of gauges
	new_gauges := make(MetricMap)
	for k, v := range a.gauges {
		new_gauges[k] = v
	}
	a.gauges = new_gauges
}

// receiveMetric is called for each incoming metric on MetricChan
func (a *MetricAggregator) receiveMetric(m Metric) {
	defer a.Unlock()
	a.Lock()

	switch m.Type {
	case COUNTER:
		v, ok := a.counters[m.Bucket]
		if ok {
			a.counters[m.Bucket] = v + m.Value
		} else {
			a.counters[m.Bucket] = m.Value
		}
	case GAUGE:
		a.gauges[m.Bucket] = m.Value
	case TIMER:
		v, ok := a.timers[m.Bucket]
		if ok {
			v = append(v, m.Value)
			a.timers[m.Bucket] = v
		} else {
			a.timers[m.Bucket] = []float64{m.Value}
		}
	case ERROR:
		a.stats.BadLines += 1
	}
	a.stats.LastMessage = time.Now()
}

// Aggregate starts the MetricAggregator so it begins consuming metrics from MetricChan
// and flushing them periodically via its Sender
func (m *MetricAggregator) Aggregate() {
	m.counters = make(MetricMap)
	m.gauges = make(MetricMap)
	m.timers = make(MetricListMap)
	flushChan := make(chan error)
	flushTimer := time.NewTimer(m.FlushInterval)

	for {
		select {
		case metric := <-m.MetricChan: // Incoming metrics
			m.receiveMetric(metric)
		case <-flushTimer.C: // Time to flush to graphite
			m.Lock()

			flushed := m.flush()
			go func() {
				e := m.Sender.SendMetrics(flushed)
				if e != nil {
					flushChan <- e
				}
			}()
			m.Reset()
			flushTimer = time.NewTimer(m.FlushInterval)

			m.Unlock()
		case flushResult := <-flushChan:
			m.Lock()

			if flushResult != nil {
				log.Printf("Sending metrics to Graphite failed: %s", flushResult)
				m.stats.GraphiteLastError = time.Now()
			} else {
				m.stats.GraphiteLastFlush = time.Now()
			}
			m.Unlock()
		}
	}

}
