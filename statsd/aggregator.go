package statsd

import (
	"math"
	"sort"
	"strconv"
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

// percentStruct is a cache of percentile names to avoid creating them for each timer.
type percentStruct struct {
	count      string
	mean       string
	sum        string
	sumSquares string
	upper      string
	lower      string
}

type aggregator struct {
	expiryInterval    time.Duration // How often to expire metrics
	lastFlush         time.Time     // Last time the metrics where aggregated
	percentThresholds map[float64]percentStruct
	aggregatorTags    types.Tags // Tags for system metrics
	aggregatorTagsStr string     // Tags for system metrics (as string)
	types.MetricMap
}

// NewAggregator creates a new Aggregator object.
func NewAggregator(percentThresholds []float64, expiryInterval time.Duration, aggregatorTags types.Tags) Aggregator {
	a := aggregator{}
	a.lastFlush = time.Now()
	a.expiryInterval = expiryInterval
	a.Counters = types.Counters{}
	a.Timers = types.Timers{}
	a.Gauges = types.Gauges{}
	a.Sets = types.Sets{}
	a.aggregatorTags = aggregatorTags
	a.aggregatorTagsStr = aggregatorTags.SortedString()
	a.percentThresholds = make(map[float64]percentStruct, len(percentThresholds))
	for _, pct := range percentThresholds {
		sPct := strconv.Itoa(int(pct))
		a.percentThresholds[pct] = percentStruct{
			count:      "count_" + sPct,
			mean:       "mean_" + sPct,
			sum:        "sum_" + sPct,
			sumSquares: "sum_squares_" + sPct,
			upper:      "upper_" + sPct,
			lower:      "lower_" + sPct,
		}
	}
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
	a.FlushInterval = startTime.Sub(a.lastFlush)
	flushInSeconds := float64(a.FlushInterval) / float64(time.Second)

	a.receiveCounter(&types.Metric{
		Name:  internalStatName("aggregator_num_stats"),
		Value: float64(a.NumStats),
		Tags:  a.aggregatorTags,
		Type:  types.COUNTER,
	}, a.aggregatorTagsStr, types.Nanotime(startTime.UnixNano()))

	a.Counters.Each(func(key, tagsKey string, counter types.Counter) {
		counter.PerSecond = float64(counter.Value) / flushInSeconds
		a.Counters[key][tagsKey] = counter
	})

	a.Timers.Each(func(key, tagsKey string, timer types.Timer) {
		if count := len(timer.Values); count > 0 {
			sort.Float64s(timer.Values)
			timer.Min = timer.Values[0]
			timer.Max = timer.Values[count-1]
			timer.Count = len(timer.Values)
			count := float64(timer.Count)

			cumulativeValues := make([]float64, timer.Count)
			cumulSumSquaresValues := make([]float64, timer.Count)
			cumulativeValues[0] = timer.Min
			cumulSumSquaresValues[0] = timer.Min * timer.Min
			for i := 1; i < timer.Count; i++ {
				cumulativeValues[i] = timer.Values[i] + cumulativeValues[i-1]
				cumulSumSquaresValues[i] = timer.Values[i]*timer.Values[i] + cumulSumSquaresValues[i-1]
			}

			var sumSquares = timer.Min * timer.Min
			var mean = timer.Min
			var sum = timer.Min
			var thresholdBoundary = timer.Max

			for pct, pctStruct := range a.percentThresholds {
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

				timer.Percentiles.Set(pctStruct.count, float64(numInThreshold))
				timer.Percentiles.Set(pctStruct.mean, mean)
				timer.Percentiles.Set(pctStruct.sum, sum)
				timer.Percentiles.Set(pctStruct.sumSquares, sumSquares)
				if pct > 0 {
					timer.Percentiles.Set(pctStruct.upper, thresholdBoundary)
				} else {
					timer.Percentiles.Set(pctStruct.lower, thresholdBoundary)
				}
			}

			sum = cumulativeValues[timer.Count-1]
			sumSquares = cumulSumSquaresValues[timer.Count-1]
			mean = sum / count

			var sumOfDiffs float64
			for i := 0; i < timer.Count; i++ {
				sumOfDiffs += (timer.Values[i] - mean) * (timer.Values[i] - mean)
			}

			mid := int(math.Floor(count / 2))
			if math.Mod(count, 2) == 0 {
				timer.Median = (timer.Values[mid-1] + timer.Values[mid]) / 2
			} else {
				timer.Median = timer.Values[mid]
			}

			timer.Mean = mean
			timer.StdDev = math.Sqrt(sumOfDiffs / count)
			timer.Sum = sum
			timer.SumSquares = sumSquares
			timer.PerSecond = count / flushInSeconds

			a.Timers[key][tagsKey] = timer
		} else {
			timer.Count = 0
			timer.PerSecond = 0
		}
	})

	flushTime := now()

	a.ProcessingTime = flushTime.Sub(startTime)

	a.receiveGauge(&types.Metric{
		Name:  internalStatName("processing_time"),
		Value: float64(a.ProcessingTime) / float64(time.Millisecond),
		Tags:  a.aggregatorTags,
		Type:  types.GAUGE,
	}, a.aggregatorTagsStr, types.Nanotime(flushTime.UnixNano()))

	a.lastFlush = flushTime
}

func (a *aggregator) Process(f ProcessFunc) {
	f(&a.MetricMap)
}

func (a *aggregator) isExpired(now, ts types.Nanotime) bool {
	return a.expiryInterval != 0 && time.Duration(now-ts) > a.expiryInterval
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
	nowNano := types.Nanotime(now.UnixNano())

	a.Counters.Each(func(key, tagsKey string, counter types.Counter) {
		if a.isExpired(nowNano, counter.Timestamp) {
			deleteMetric(key, tagsKey, a.Counters)
		} else {
			a.Counters[key][tagsKey] = types.Counter{
				Timestamp: counter.Timestamp,
				Hostname:  counter.Hostname,
				Tags:      counter.Tags,
			}
		}
	})

	a.Timers.Each(func(key, tagsKey string, timer types.Timer) {
		if a.isExpired(nowNano, timer.Timestamp) {
			deleteMetric(key, tagsKey, a.Timers)
		} else {
			a.Timers[key][tagsKey] = types.Timer{
				Timestamp: timer.Timestamp,
				Hostname:  timer.Hostname,
				Tags:      timer.Tags,
			}
		}
	})

	a.Gauges.Each(func(key, tagsKey string, gauge types.Gauge) {
		if a.isExpired(nowNano, gauge.Timestamp) {
			deleteMetric(key, tagsKey, a.Gauges)
		}
		// No reset for gauges, they keep the last value until expiration
	})

	a.Sets.Each(func(key, tagsKey string, set types.Set) {
		if a.isExpired(nowNano, set.Timestamp) {
			deleteMetric(key, tagsKey, a.Sets)
		} else {
			a.Sets[key][tagsKey] = types.Set{
				Values:    make(map[string]struct{}),
				Timestamp: set.Timestamp,
				Hostname:  set.Hostname,
				Tags:      set.Tags,
			}
		}
	})
}

func (a *aggregator) receiveCounter(m *types.Metric, tagsKey string, now types.Nanotime) {
	value := int64(m.Value)
	v, ok := a.Counters[m.Name]
	if ok {
		c, ok := v[tagsKey]
		if ok {
			c.Value += value
			c.Timestamp = now
		} else {
			c = types.NewCounter(now, value, m.Hostname, m.Tags)
		}
		v[tagsKey] = c
	} else {
		a.Counters[m.Name] = map[string]types.Counter{
			tagsKey: types.NewCounter(now, value, m.Hostname, m.Tags),
		}
	}
}

func (a *aggregator) receiveGauge(m *types.Metric, tagsKey string, now types.Nanotime) {
	// TODO: handle +/-
	v, ok := a.Gauges[m.Name]
	if ok {
		g, ok := v[tagsKey]
		if ok {
			g.Value = m.Value
			g.Timestamp = now
		} else {
			g = types.NewGauge(now, m.Value, m.Hostname, m.Tags)
		}
		v[tagsKey] = g
	} else {
		a.Gauges[m.Name] = map[string]types.Gauge{
			tagsKey: types.NewGauge(now, m.Value, m.Hostname, m.Tags),
		}
	}
}

func (a *aggregator) receiveTimer(m *types.Metric, tagsKey string, now types.Nanotime) {
	v, ok := a.Timers[m.Name]
	if ok {
		t, ok := v[tagsKey]
		if ok {
			t.Values = append(t.Values, m.Value)
			t.Timestamp = now
		} else {
			t = types.NewTimer(now, []float64{m.Value}, m.Hostname, m.Tags)
		}
		v[tagsKey] = t
	} else {
		a.Timers[m.Name] = map[string]types.Timer{
			tagsKey: types.NewTimer(now, []float64{m.Value}, m.Hostname, m.Tags),
		}
	}
}

func (a *aggregator) receiveSet(m *types.Metric, tagsKey string, now types.Nanotime) {
	v, ok := a.Sets[m.Name]
	if ok {
		s, ok := v[tagsKey]
		if ok {
			s.Values[m.StringValue] = struct{}{}
			s.Timestamp = now
		} else {
			s = types.NewSet(now, map[string]struct{}{m.StringValue: {}}, m.Hostname, m.Tags)
		}
		v[tagsKey] = s
	} else {
		a.Sets[m.Name] = map[string]types.Set{
			tagsKey: types.NewSet(now, map[string]struct{}{m.StringValue: {}}, m.Hostname, m.Tags),
		}
	}
}

// Receive aggregates an incoming metric.
func (a *aggregator) Receive(m *types.Metric, now time.Time) {
	a.NumStats++
	tagsKey := formatTagsKey(m.Tags, m.Hostname)
	nowNano := types.Nanotime(now.UnixNano())

	switch m.Type {
	case types.COUNTER:
		a.receiveCounter(m, tagsKey, nowNano)
	case types.GAUGE:
		a.receiveGauge(m, tagsKey, nowNano)
	case types.TIMER:
		a.receiveTimer(m, tagsKey, nowNano)
	case types.SET:
		a.receiveSet(m, tagsKey, nowNano)
	default:
		log.Errorf("Unknow metric type %s for %s", m.Type, m.Name)
	}
}

func formatTagsKey(tags types.Tags, hostname string) string {
	t := tags.SortedString()
	if hostname == "" {
		return t
	}
	return t + "," + types.StatsdSourceID + ":" + hostname
}
