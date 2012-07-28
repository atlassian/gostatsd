package statsd

import (
	"log"
	"sort"
	"strconv"
	"sync"
	"time"
)

var percentThresholds []float64

func init() {
	percentThresholds = []float64{90.0}
}

type MetricAggregatorStats struct {
	BadLines          int
	LastMessage       time.Time
	GraphiteLastFlush time.Time
	GraphiteLastError time.Time
}

func thresholdStats(vals []float64, threshold float64) (mean, upper float64) {
	if count := len(vals); count > 1 {
		idx := int(round(((100 - threshold) / 100) * float64(count)))
		thresholdCount := count - idx
		thresholdValues := vals[:thresholdCount]

		mean = average(thresholdValues)
		upper = thresholdValues[len(thresholdValues)-1]
	} else {
		mean = vals[0]
		upper = vals[0]
	}
	return mean, upper
}

type MetricSender interface {
	SendMetrics(MetricMap) error
}

type MetricAggregator struct {
	sync.Mutex
	MetricChan    chan Metric   // Channel on which metrics are received
	FlushInterval time.Duration // How often to flush metrics to the sender
	Sender        MetricSender
	stats         MetricAggregatorStats
	counters      MetricMap
	gauges        MetricMap
	timers        MetricListMap
}

func NewMetricAggregator(sender MetricSender, flushInterval time.Duration) (a MetricAggregator) {
	a = MetricAggregator{}
	a.FlushInterval = flushInterval
	a.Sender = sender
	a.MetricChan = make(chan Metric)
	return
}

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

			for _, threshold := range percentThresholds {
				mean, upper := thresholdStats(v, threshold)
				thresholdName := strconv.FormatFloat(threshold, 'f', 1, 64)
				metrics["stats.timers."+k+"mean_"+thresholdName] = mean
				metrics["stats.timers."+k+"upper_"+thresholdName] = upper
			}
			numStats += 1
		}
	}
	metrics["statsd.numStats"] = float64(numStats)
	return metrics
}

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
