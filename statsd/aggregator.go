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
	types.MetricMap
}

// NewMetricAggregator creates a new MetricAggregator object
func NewMetricAggregator(senders []backend.MetricSender, flushInterval time.Duration) MetricAggregator {
	a := MetricAggregator{}
	a.FlushInterval = flushInterval
	a.Senders = senders
	a.MetricChan = make(chan types.Metric)
	return a
}

// flush prepares the contents of a MetricAggregator for sending via the Sender
func (a *MetricAggregator) flush() (metrics types.MetricMap) {
	defer a.Unlock()
	a.Lock()

	numStats := 0

	types.EachCounter(a.Counters, func(key, tagsKey string, counter types.Counter) {
		perSecond := float64(counter.Value) / a.FlushInterval.Seconds()
		counter.PerSecond = perSecond
		a.Counters[key][tagsKey] = counter
		numStats += 1
	})

	for _, gauges := range a.Gauges {
		numStats += len(gauges)
	}

	types.EachTimer(a.Timers, func(key, tagsKey string, timer types.Timer) {
		if count := len(timer.Values); count > 0 {
			sort.Float64s(timer.Values)
			timer.Min = timer.Values[0]
			timer.Max = timer.Values[count-1]
			timer.Count = len(timer.Values)
			a.Timers[key][tagsKey] = timer
			numStats += 1
		}
	})

	return types.MetricMap{
		NumStats: numStats,
		Counters: types.CopyCounters(a.Counters),
		Timers:   types.CopyTimers(a.Timers),
		Gauges:   types.CopyGauges(a.Gauges),
	}
}

// Reset clears the contents of a MetricAggregator
func (a *MetricAggregator) Reset() {
	defer a.Unlock()
	a.Lock()
	a.NumStats = 0

	types.EachCounter(a.Counters, func(key, tagsKey string, counter types.Counter) {
		counter.Value = 0
		counter.PerSecond = 0
		a.Counters[key][tagsKey] = counter
	})

	types.EachTimer(a.Timers, func(key, tagsKey string, timer types.Timer) {
		timer.Values = []float64{}
		timer.Min = 0
		timer.Max = 0
		a.Timers[key][tagsKey] = timer
	})

	// No reset for gauges, they keep the last value
}

// receiveMetric is called for each incoming metric on MetricChan
func (a *MetricAggregator) receiveMetric(m types.Metric) {
	defer a.Unlock()
	a.Lock()

	tagsKey := m.Tags.String()
	switch m.Type {
	case types.COUNTER:
		v, ok := a.Counters[m.Name]
		if ok {
			c, ok := v[tagsKey]
			if ok {
				c.Value = c.Value + int64(m.Value)
				a.Counters[m.Name][tagsKey] = c
			} else {
				a.Counters[m.Name] = make(map[string]types.Counter)
				a.Counters[m.Name][tagsKey] = types.Counter{Value: int64(m.Value)}
			}
		} else {
			a.Counters = make(map[string]map[string]types.Counter)
			a.Counters[m.Name] = make(map[string]types.Counter)
			a.Counters[m.Name][tagsKey] = types.Counter{Value: int64(m.Value)}
		}
	case types.GAUGE:
		// TODO: handle +/-
		v, ok := a.Gauges[m.Name]
		if ok {
			g, ok := v[tagsKey]
			if ok {
				g.Value = m.Value
				a.Gauges[m.Name][tagsKey] = g
			} else {
				a.Gauges[m.Name] = make(map[string]types.Gauge)
				a.Gauges[m.Name][tagsKey] = types.Gauge{Value: m.Value}
			}
		} else {
			a.Gauges = make(map[string]map[string]types.Gauge)
			a.Gauges[m.Name] = make(map[string]types.Gauge)
			a.Gauges[m.Name][tagsKey] = types.Gauge{Value: m.Value}
		}
	case types.TIMER:
		v, ok := a.Timers[m.Name]
		if ok {
			t, ok := v[tagsKey]
			if ok {
				t.Values = append(t.Values, m.Value)
				a.Timers[m.Name][tagsKey] = t
			} else {
				a.Timers[m.Name] = make(map[string]types.Timer)
				a.Timers[m.Name][tagsKey] = types.Timer{Values: []float64{m.Value}}
			}
		} else {
			a.Timers = make(map[string]map[string]types.Timer)
			a.Timers[m.Name] = make(map[string]types.Timer)
			a.Timers[m.Name][tagsKey] = types.Timer{Values: []float64{m.Value}}
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
		case <-flushTimer.C: // Time to flush to the backends
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
				log.Printf("Sending metrics to backend failed: %s", flushResult)
				a.Stats.LastFlushError = time.Now()
			} else {
				a.Stats.LastFlush = time.Now()
			}
			a.Unlock()
		}
	}

}
