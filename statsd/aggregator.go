package statsd

import (
	"log"
	"sort"
	"sync"
	"time"
)

// metricAggregatorStats is a bookkeeping structure for statistics about a MetricAggregator
type metricAggregatorStats struct {
	BadLines       int
	LastMessage    time.Time
	LastFlush      time.Time
	LastFlushError time.Time
}

// MetricSender is an interface that can be implemented by objects which
// can provide metrics to a MetricAggregator
type MetricSender interface {
	SendMetrics(MetricMap) error
}

// The MetricSenderFunc type is an adapter to allow the use of ordinary functions as metric senders
type MetricSenderFunc func(MetricMap) error

// SendMetrics calls f(m)
func (f MetricSenderFunc) SendMetrics(m MetricMap) error {
	return f(m)
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
	Stats         metricAggregatorStats
	Counters      MetricMap
	Gauges        MetricMap
	Timers        MetricListMap
}

// NewMetricAggregator creates a new MetricAggregator object
func NewMetricAggregator(sender MetricSender, flushInterval time.Duration) MetricAggregator {
	a := MetricAggregator{}
	a.FlushInterval = flushInterval
	a.Sender = sender
	a.MetricChan = make(chan Metric)
	a.Counters = make(MetricMap)
	a.Gauges = make(MetricMap)
	a.Timers = make(MetricListMap)
	return a
}

// flush prepares the contents of a MetricAggregator for sending via the Sender
func (a *MetricAggregator) flush() (metrics MetricMap) {
	defer a.Unlock()
	a.Lock()

	metrics = make(MetricMap)
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
func (a *MetricAggregator) receiveMetric(m Metric) {
	defer a.Unlock()
	a.Lock()

	switch m.Type {
	case COUNTER:
		v, ok := a.Counters[m.Bucket]
		if ok {
			a.Counters[m.Bucket] = v + m.Value
		} else {
			a.Counters[m.Bucket] = m.Value
		}
	case GAUGE:
		a.Gauges[m.Bucket] = m.Value
	case TIMER:
		v, ok := a.Timers[m.Bucket]
		if ok {
			v = append(v, m.Value)
			a.Timers[m.Bucket] = v
		} else {
			a.Timers[m.Bucket] = []float64{m.Value}
		}
	case ERROR:
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
			go func() {
				flushChan <- a.Sender.SendMetrics(flushed)
			}()
			a.Reset()
			flushTimer = time.NewTimer(a.FlushInterval)
		case flushResult := <-flushChan:
			a.Lock()

			if flushResult != nil {
				log.Printf("Sending metrics to Graphite failed: %s", flushResult)
				a.Stats.LastFlushError = time.Now()
			} else {
				a.Stats.LastFlush = time.Now()
			}
			a.Unlock()
		}
	}

}
