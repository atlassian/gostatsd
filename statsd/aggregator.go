package statsd

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jtblin/gostatsd/backend"
	"github.com/jtblin/gostatsd/types"

	log "github.com/Sirupsen/logrus"
)

// metricAggregatorStats is a bookkeeping structure for statistics about a MetricAggregator.
type metricAggregatorStats struct {
	BadLines       int64
	LastMessage    time.Time
	LastFlush      time.Time
	LastFlushError time.Time
	NumStats       int
	ProcessingTime time.Duration
}

// MetricAggregator is an object that aggregates statsd metrics.
// The function NewMetricAggregator should be used to create the objects.
//
// Incoming metrics should be sent to the MetricQueue channel.
type MetricAggregator struct {
	sync.Mutex
	ExpiryInterval    time.Duration      // How often to expire metrics
	FlushInterval     time.Duration      // How often to flush metrics to the sender
	LastFlush         time.Time          // Last time the metrics where aggregated
	MaxWorkers        int                // Number of workers to metrics queue
	MetricQueue       chan *types.Metric // Queue on which metrics are received
	PercentThresholds []float64
	Senders           []backend.MetricSender // The sender to which metrics are flushed
	Stats             metricAggregatorStats
	Tags              types.Tags // Tags to add to all metrics
	types.MetricMap
}

// NewMetricAggregator creates a new MetricAggregator object.
func NewMetricAggregator(senders []backend.MetricSender, percentThresholds []float64, flushInterval time.Duration, expiryInterval time.Duration, maxWorkers int, tags []string) *MetricAggregator {
	a := MetricAggregator{}
	a.FlushInterval = flushInterval
	a.LastFlush = time.Now()
	a.ExpiryInterval = expiryInterval
	a.Senders = senders
	a.MetricQueue = make(chan *types.Metric, maxQueueSize*10) // we are going to receive more metrics than messages
	a.MaxWorkers = maxWorkers
	a.PercentThresholds = percentThresholds
	a.Counters = types.Counters{}
	a.Timers = types.Timers{}
	a.Gauges = types.Gauges{}
	a.Sets = types.Sets{}
	a.Tags = tags
	return &a
}

// round rounds a number to its nearest integer value.
// poor man's math.Round(x) = math.Floor(x + 0.5).
func round(v float64) float64 {
	return math.Floor(v + 0.5)
}

// flush prepares the contents of a MetricAggregator for sending via the Sender.
func (a *MetricAggregator) flush(now func() time.Time) (metrics types.MetricMap) {
	a.Lock()
	defer a.Unlock()

	startTime := now()
	flushInterval := startTime.Sub(a.LastFlush)

	a.Counters.Each(func(key, tagsKey string, counter types.Counter) {
		perSecond := float64(counter.Value) / flushInterval.Seconds()
		counter.PerSecond = perSecond
		a.Counters[key][tagsKey] = counter
	})

	a.Timers.Each(func(key, tagsKey string, timer types.Timer) {
		if count := len(timer.Values); count > 0 {
			sort.Float64s(timer.Values)
			timer.Min = timer.Values[0]
			timer.Max = timer.Values[count-1]
			timer.Count = len(timer.Values)
			count := float64(timer.Count)

			cumulativeValues := []float64{timer.Min}
			cumulSumSquaresValues := []float64{timer.Min * timer.Min}
			for i := 1; i < timer.Count; i++ {
				cumulativeValues = append(cumulativeValues, timer.Values[i]+cumulativeValues[i-1])
				cumulSumSquaresValues = append(cumulSumSquaresValues,
					timer.Values[i]*timer.Values[i]+cumulSumSquaresValues[i-1])
			}

			var sumSquares = timer.Min * timer.Min
			var mean = timer.Min
			var sum = timer.Min
			var thresholdBoundary = timer.Max

			for _, pct := range a.PercentThresholds {
				numInThreshold := timer.Count
				if timer.Count > 1 {
					numInThreshold = int(round(math.Abs(pct) / 100 * count))
					if numInThreshold == 0 {
						continue
					}
					if pct > 0 {
						thresholdBoundary = timer.Values[numInThreshold-1]
						sum = cumulativeValues[numInThreshold-1]
						sumSquares = cumulSumSquaresValues[numInThreshold-1]
					} else {
						thresholdBoundary = timer.Values[timer.Count-numInThreshold]
						sum = cumulativeValues[timer.Count-1] - cumulativeValues[timer.Count-numInThreshold-1]
						sumSquares = cumulSumSquaresValues[timer.Count-1] - cumulSumSquaresValues[timer.Count-numInThreshold-1]
					}
					mean = sum / float64(numInThreshold)
				}

				sPct := fmt.Sprintf("%d", int(pct))
				timer.Percentiles.Set(fmt.Sprintf("count_%s", sPct), float64(numInThreshold))
				timer.Percentiles.Set(fmt.Sprintf("mean_%s", sPct), mean)
				timer.Percentiles.Set(fmt.Sprintf("sum_%s", sPct), sum)
				timer.Percentiles.Set(fmt.Sprintf("sum_squares_%s", sPct), sumSquares)
				if pct > 0 {
					timer.Percentiles.Set(fmt.Sprintf("upper_%s", sPct), thresholdBoundary)
				} else {
					timer.Percentiles.Set(fmt.Sprintf("lower_%s", sPct), thresholdBoundary)
				}
			}

			sum = cumulativeValues[timer.Count-1]
			sumSquares = cumulSumSquaresValues[timer.Count-1]
			mean = sum / count

			var sumOfDiffs = float64(0)
			for i := 0; i < timer.Count; i++ {
				sumOfDiffs += (timer.Values[i] - mean) * (timer.Values[i] - mean)
			}

			mid := int(math.Floor(count / 2))
			if math.Mod(count, float64(2)) == 0 {
				timer.Median = (timer.Values[mid-1] + timer.Values[mid]) / 2
			} else {
				timer.Median = timer.Values[mid]
			}

			timer.Mean = mean
			timer.StdDev = math.Sqrt(sumOfDiffs / count)
			timer.Sum = sum
			timer.SumSquares = sumSquares
			timer.PerSecond = count / flushInterval.Seconds()

			a.Timers[key][tagsKey] = timer
		} else {
			timer.Count = 0
			timer.PerSecond = float64(0)
		}
	})

	tags := a.Tags.String()
	a.Stats.NumStats = a.NumStats
	a.Stats.ProcessingTime = now().Sub(startTime)

	statName := internalStatName("numStats")
	a.receiveCounter(statName, tags, int64(a.NumStats), now())
	m := a.Counters[statName][tags]
	m.PerSecond = float64(a.NumStats) / flushInterval.Seconds()
	a.Counters[statName][tags] = m

	statName = internalStatName("processingTime")
	a.receiveGauge(statName, tags, float64(a.Stats.ProcessingTime)/float64(time.Millisecond), now())

	if badLines, ok := a.Counters[internalStatName("bad_lines_seen")][tags]; ok {
		a.Stats.BadLines += badLines.Value
	}

	a.LastFlush = now()

	return types.MetricMap{
		NumStats:       a.Stats.NumStats,
		ProcessingTime: a.Stats.ProcessingTime,
		FlushInterval:  flushInterval,
		Counters:       a.Counters.Clone(),
		Timers:         a.Timers.Clone(),
		Gauges:         a.Gauges.Clone(),
		Sets:           a.Sets.Clone(),
	}
}

func (a *MetricAggregator) isExpired(now, ts time.Time) bool {
	return a.ExpiryInterval != time.Duration(0) && now.Sub(ts) > a.ExpiryInterval
}

func (a *MetricAggregator) deleteMetric(key, tagsKey string, metrics types.AggregatedMetrics) {
	metrics.DeleteChild(key, tagsKey)
	if !metrics.HasChildren(key) {
		metrics.Delete(key)
	}
}

// Reset clears the contents of a MetricAggregator
func (a *MetricAggregator) Reset(now time.Time) {
	a.Lock()
	defer a.Unlock()
	a.NumStats = 0

	a.Counters.Each(func(key, tagsKey string, counter types.Counter) {
		if a.isExpired(now, counter.Timestamp) {
			a.deleteMetric(key, tagsKey, a.Counters)
		} else {
			interval := counter.Interval
			a.Counters[key][tagsKey] = types.Counter{Interval: interval}
		}
	})

	a.Timers.Each(func(key, tagsKey string, timer types.Timer) {
		if a.isExpired(now, timer.Timestamp) {
			a.deleteMetric(key, tagsKey, a.Timers)
		} else {
			interval := timer.Interval
			a.Timers[key][tagsKey] = types.Timer{Interval: interval}
		}
	})

	a.Gauges.Each(func(key, tagsKey string, gauge types.Gauge) {
		if a.isExpired(now, gauge.Timestamp) {
			a.deleteMetric(key, tagsKey, a.Gauges)
		}
		// No reset for gauges, they keep the last value until expiration
	})

	a.Sets.Each(func(key, tagsKey string, set types.Set) {
		if a.isExpired(now, set.Timestamp) {
			a.deleteMetric(key, tagsKey, a.Sets)
		} else {
			interval := set.Interval
			a.Sets[key][tagsKey] = types.Set{Interval: interval, Values: make(map[string]int64)}
		}
	})
}

func (a *MetricAggregator) receiveCounter(name, tags string, value int64, now time.Time) {
	v, ok := a.Counters[name]
	if ok {
		c, ok := v[tags]
		if ok {
			c.Value = c.Value + value
			a.Counters[name][tags] = c
		} else {
			a.Counters[name][tags] = types.NewCounter(now, a.FlushInterval, value)
		}
	} else {
		a.Counters[name] = make(map[string]types.Counter)
		a.Counters[name][tags] = types.NewCounter(now, a.FlushInterval, value)
	}
}

func (a *MetricAggregator) receiveGauge(name, tags string, value float64, now time.Time) {
	// TODO: handle +/-
	v, ok := a.Gauges[name]
	if ok {
		g, ok := v[tags]
		if ok {
			g.Value = value
			a.Gauges[name][tags] = g
		} else {
			a.Gauges[name][tags] = types.NewGauge(now, a.FlushInterval, value)
		}
	} else {
		a.Gauges[name] = make(map[string]types.Gauge)
		a.Gauges[name][tags] = types.NewGauge(now, a.FlushInterval, value)
	}
}

func (a *MetricAggregator) receiveTimer(name, tags string, value float64, now time.Time) {
	v, ok := a.Timers[name]
	if ok {
		t, ok := v[tags]
		if ok {
			t.Values = append(t.Values, value)
			a.Timers[name][tags] = t
		} else {
			a.Timers[name][tags] = types.NewTimer(now, a.FlushInterval, []float64{value})
		}
	} else {
		a.Timers[name] = make(map[string]types.Timer)
		a.Timers[name][tags] = types.NewTimer(now, a.FlushInterval, []float64{value})
	}
}

func (a *MetricAggregator) receiveSet(name, tags string, value string, now time.Time) {
	v, ok := a.Sets[name]
	if ok {
		s, ok := v[tags]
		if ok {
			_, ok := s.Values[value]
			if ok {
				s.Values[value]++
			} else {
				s.Values[value] = 1
			}
			a.Sets[name][tags] = s
		} else {
			unique := make(map[string]int64)
			unique[value] = 1
			a.Sets[name][tags] = types.NewSet(now, a.FlushInterval, unique)
		}
	} else {
		a.Sets[name] = make(map[string]types.Set)
		unique := make(map[string]int64)
		unique[value] = 1
		a.Sets[name][tags] = types.NewSet(now, a.FlushInterval, unique)
	}
}

// receiveMetric is called for each incoming metric on MetricChan.
func (a *MetricAggregator) receiveMetric(m *types.Metric, now time.Time) {
	a.Lock()
	defer a.Unlock()

	if !strings.HasPrefix(m.Name, internalStatName("")) {
		a.NumStats++
	}
	tagsKey := m.Tags.String()

	switch m.Type {
	case types.COUNTER:
		a.receiveCounter(m.Name, tagsKey, int64(m.Value), now)
	case types.GAUGE:
		a.receiveGauge(m.Name, tagsKey, m.Value, now)
	case types.TIMER:
		a.receiveTimer(m.Name, tagsKey, m.Value, now)
	case types.SET:
		a.receiveSet(m.Name, tagsKey, m.StringValue, now)
	default:
		log.Errorf("Unknow metric type %s for %s", m.Type, m.Name)
	}

	a.Stats.LastMessage = time.Now()
}

func (a *MetricAggregator) processQueue() {
	for metric := range a.MetricQueue {
		a.receiveMetric(metric, time.Now())
		runtime.Gosched()
	}
}

// Aggregate starts the MetricAggregator so it begins consuming metrics from MetricChan
// and flushing them periodically via its Sender.
func (a *MetricAggregator) Aggregate() {
	flushChan := make(chan error)
	flushTimer := time.NewTimer(a.FlushInterval)

	for i := 0; i < a.MaxWorkers; i++ {
		go a.processQueue()
	}

	for {
		select {
		case <-flushTimer.C: // Time to flush to the backends
			flushed := a.flush(time.Now) // pass func for stubbing
			a.Reset(time.Now())
			for _, sender := range a.Senders {
				s := sender
				go func() {
					log.Debugf("Send metrics to backend %s", s.BackendName())
					flushChan <- s.SendMetrics(flushed)
				}()
			}
			flushTimer = time.NewTimer(a.FlushInterval)
		case flushResult := <-flushChan:
			a.Lock()
			if flushResult != nil {
				log.Errorf("Sending metrics to backend failed: %s", flushResult)
				a.Stats.LastFlushError = time.Now()
			} else {
				a.Stats.LastFlush = time.Now()
			}
			a.Unlock()
		}
	}
}
