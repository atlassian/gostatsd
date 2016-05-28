package statsd

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
)

// ProcessFunc is a function that gets executed by Aggregator with its state passed into the function.
type ProcessFunc func(*types.MetricMap)

// Aggregator is an object that aggregates statsd metrics.
// The function NewAggregator should be used to create the objects.
//
// Incoming metrics should be passed via Receive function.
type Aggregator interface {
	Receive(*types.Metric, time.Time)
	Flush(func() time.Time)
	Process(ProcessFunc)
	Reset(time.Time)
}

type aggregator struct {
	expiryInterval    time.Duration // How often to expire metrics
	lastFlush         time.Time     // Last time the metrics where aggregated
	percentThresholds []float64
	defaultTags       string // Tags to add to system metrics
	types.MetricMap
}

// NewAggregator creates a new Aggregator object.
func NewAggregator(percentThresholds []float64, flushInterval, expiryInterval time.Duration, defaultTags []string) Aggregator {
	a := aggregator{}
	a.FlushInterval = flushInterval
	a.lastFlush = time.Now()
	a.expiryInterval = expiryInterval
	a.percentThresholds = percentThresholds
	a.Counters = types.Counters{}
	a.Timers = types.Timers{}
	a.Gauges = types.Gauges{}
	a.Sets = types.Sets{}
	a.defaultTags = types.Tags(defaultTags).String()
	return &a
}

// round rounds a number to its nearest integer value.
// poor man's math.Round(x) = math.Floor(x + 0.5).
func round(v float64) float64 {
	return math.Floor(v + 0.5)
}

// Flush prepares the contents of an Aggregator for sending via the Sender.
func (a *aggregator) Flush(now func() time.Time) {
	startTime := now()
	flushInterval := startTime.Sub(a.lastFlush)

	statName := internalStatName("aggregator_num_stats")
	a.receiveCounter(statName, a.defaultTags, int64(a.NumStats), startTime)

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

			for _, pct := range a.percentThresholds {
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

	flushTime := now()

	a.ProcessingTime = flushTime.Sub(startTime)

	statName = internalStatName("processing_time")
	a.receiveGauge(statName, a.defaultTags, float64(a.ProcessingTime)/float64(time.Millisecond), flushTime)

	a.lastFlush = flushTime
}

func (a *aggregator) Process(f ProcessFunc) {
	f(&a.MetricMap)
}

func (a *aggregator) isExpired(now, ts time.Time) bool {
	return a.expiryInterval != time.Duration(0) && now.Sub(ts) > a.expiryInterval
}

func deleteMetric(key, tagsKey string, metrics types.AggregatedMetrics) {
	metrics.DeleteChild(key, tagsKey)
	if !metrics.HasChildren(key) {
		metrics.Delete(key)
	}
}

// Reset clears the contents of an Aggregator.
func (a *aggregator) Reset(now time.Time) {
	a.NumStats = 0

	a.Counters.Each(func(key, tagsKey string, counter types.Counter) {
		if a.isExpired(now, counter.Timestamp) {
			deleteMetric(key, tagsKey, a.Counters)
		} else {
			interval := counter.Interval
			a.Counters[key][tagsKey] = types.Counter{Interval: interval}
		}
	})

	a.Timers.Each(func(key, tagsKey string, timer types.Timer) {
		if a.isExpired(now, timer.Timestamp) {
			deleteMetric(key, tagsKey, a.Timers)
		} else {
			interval := timer.Interval
			a.Timers[key][tagsKey] = types.Timer{Interval: interval}
		}
	})

	a.Gauges.Each(func(key, tagsKey string, gauge types.Gauge) {
		if a.isExpired(now, gauge.Timestamp) {
			deleteMetric(key, tagsKey, a.Gauges)
		}
		// No reset for gauges, they keep the last value until expiration
	})

	a.Sets.Each(func(key, tagsKey string, set types.Set) {
		if a.isExpired(now, set.Timestamp) {
			deleteMetric(key, tagsKey, a.Sets)
		} else {
			interval := set.Interval
			a.Sets[key][tagsKey] = types.Set{Interval: interval, Values: make(map[string]int64)}
		}
	})
}

func (a *aggregator) receiveCounter(name, tags string, value int64, now time.Time) {
	v, ok := a.Counters[name]
	if ok {
		c, ok := v[tags]
		if ok {
			c.Value += value
			a.Counters[name][tags] = c
		} else {
			a.Counters[name][tags] = types.NewCounter(now, a.FlushInterval, value)
		}
	} else {
		a.Counters[name] = make(map[string]types.Counter)
		a.Counters[name][tags] = types.NewCounter(now, a.FlushInterval, value)
	}
}

func (a *aggregator) receiveGauge(name, tags string, value float64, now time.Time) {
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

func (a *aggregator) receiveTimer(name, tags string, value float64, now time.Time) {
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

func (a *aggregator) receiveSet(name, tags string, value string, now time.Time) {
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

// Receive aggregates an incoming metric.
func (a *aggregator) Receive(m *types.Metric, now time.Time) {
	a.NumStats++
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
}
