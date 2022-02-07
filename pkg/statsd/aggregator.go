package statsd

import (
	"context"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"
)

// percentStruct is a cache of percentile names to avoid creating them for each timer.
type percentStruct struct {
	count      string
	mean       string
	sum        string
	sumSquares string
	upper      string
	lower      string
}

// MetricAggregator aggregates metrics.
type MetricAggregator struct {
	metricMapsReceived    uint64
	expiryIntervalCounter time.Duration // How often to expire counters
	expiryIntervalGauge   time.Duration // How often to expire gauges
	expiryIntervalSet     time.Duration // How often to expire sets
	expiryIntervalTimer   time.Duration // How often to expire timers
	percentThresholds     map[float64]percentStruct
	now                   func() time.Time // Returns current time. Useful for testing.
	statser               stats.Statser
	disabledSubtypes      gostatsd.TimerSubtypes
	histogramLimit        uint32
	metricMap             *gostatsd.MetricMap
}

// NewMetricAggregator creates a new MetricAggregator object.
func NewMetricAggregator(
	percentThresholds []float64,
	expiryIntervalCounter time.Duration,
	expiryIntervalGauge time.Duration,
	expiryIntervalSet time.Duration,
	expiryIntervalTimer time.Duration,
	disabled gostatsd.TimerSubtypes,
	histogramLimit uint32,
) *MetricAggregator {
	a := MetricAggregator{
		expiryIntervalCounter: expiryIntervalCounter,
		expiryIntervalGauge:   expiryIntervalGauge,
		expiryIntervalSet:     expiryIntervalSet,
		expiryIntervalTimer:   expiryIntervalTimer,

		percentThresholds: make(map[float64]percentStruct, len(percentThresholds)),
		now:               time.Now,
		statser:           stats.NewNullStatser(), // Will probably be replaced via RunMetrics
		metricMap:         gostatsd.NewMetricMap(false),
		disabledSubtypes:  disabled,
		histogramLimit:    histogramLimit,
	}
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

// Flush prepares the contents of a MetricAggregator for sending via the Sender.
func (a *MetricAggregator) Flush(flushInterval time.Duration) {
	a.statser.Gauge("aggregator.metricmaps_received", float64(a.metricMapsReceived), nil)

	flushInSeconds := float64(flushInterval) / float64(time.Second)

	a.metricMap.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		counter.PerSecond = float64(counter.Value) / flushInSeconds
		a.metricMap.Counters[key][tagsKey] = counter
	})

	a.metricMap.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		if hasHistogramTag(timer) {
			timer.Histogram = latencyHistogram(timer, a.histogramLimit)
			a.metricMap.Timers[key][tagsKey] = timer
			return
		}

		if count := len(timer.Values); count > 0 {
			sort.Float64s(timer.Values)
			timer.Min = timer.Values[0]
			timer.Max = timer.Values[count-1]
			n := len(timer.Values)
			count := float64(n)

			cumulativeValues := make([]float64, n)
			cumulSumSquaresValues := make([]float64, n)
			cumulativeValues[0] = timer.Min
			cumulSumSquaresValues[0] = timer.Min * timer.Min
			for i := 1; i < n; i++ {
				cumulativeValues[i] = timer.Values[i] + cumulativeValues[i-1]
				cumulSumSquaresValues[i] = timer.Values[i]*timer.Values[i] + cumulSumSquaresValues[i-1]
			}

			var sumSquares = timer.Min * timer.Min
			var mean = timer.Min
			var sum = timer.Min
			var thresholdBoundary = timer.Max

			for pct, pctStruct := range a.percentThresholds {
				numInThreshold := n
				if n > 1 {
					numInThreshold = int(round(math.Abs(pct) / 100 * count))
					if numInThreshold == 0 {
						continue
					}
					if pct > 0 {
						thresholdBoundary = timer.Values[numInThreshold-1]
						sum = cumulativeValues[numInThreshold-1]
						sumSquares = cumulSumSquaresValues[numInThreshold-1]
					} else {
						thresholdBoundary = timer.Values[n-numInThreshold]
						sum = cumulativeValues[n-1] - cumulativeValues[n-numInThreshold-1]
						sumSquares = cumulSumSquaresValues[n-1] - cumulSumSquaresValues[n-numInThreshold-1]
					}
					mean = sum / float64(numInThreshold)
				}

				if !a.disabledSubtypes.CountPct {
					timer.Percentiles.Set(pctStruct.count, float64(numInThreshold))
				}
				if !a.disabledSubtypes.MeanPct {
					timer.Percentiles.Set(pctStruct.mean, mean)
				}
				if !a.disabledSubtypes.SumPct {
					timer.Percentiles.Set(pctStruct.sum, sum)
				}
				if !a.disabledSubtypes.SumSquaresPct {
					timer.Percentiles.Set(pctStruct.sumSquares, sumSquares)
				}
				if pct > 0 {
					if !a.disabledSubtypes.UpperPct {
						timer.Percentiles.Set(pctStruct.upper, thresholdBoundary)
					}
				} else {
					if !a.disabledSubtypes.LowerPct {
						timer.Percentiles.Set(pctStruct.lower, thresholdBoundary)
					}
				}
			}

			sum = cumulativeValues[n-1]
			sumSquares = cumulSumSquaresValues[n-1]
			mean = sum / count

			var sumOfDiffs float64
			for i := 0; i < n; i++ {
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

			timer.Count = int(round(timer.SampledCount))
			timer.PerSecond = timer.SampledCount / flushInSeconds
		} else {
			timer.Count = 0
			timer.SampledCount = 0
			timer.PerSecond = 0
		}
		a.metricMap.Timers[key][tagsKey] = timer
	})
}

func (a *MetricAggregator) RunMetrics(ctx context.Context, statser stats.Statser) {
	a.statser = statser
}

func (a *MetricAggregator) Process(f ProcessFunc) {
	f(a.metricMap)
}

func isExpired(interval time.Duration, now, ts gostatsd.Nanotime) bool {
	return interval != 0 && time.Duration(now-ts) > interval
}

func deleteMetric(key, tagsKey string, metrics gostatsd.AggregatedMetrics) {
	metrics.DeleteChild(key, tagsKey)
	if !metrics.HasChildren(key) {
		metrics.Delete(key)
	}
}

// Reset clears the contents of a MetricAggregator.
func (a *MetricAggregator) Reset() {
	a.metricMapsReceived = 0
	nowNano := gostatsd.Nanotime(a.now().UnixNano())

	a.metricMap.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		if isExpired(a.expiryIntervalCounter, nowNano, counter.Timestamp) {
			deleteMetric(key, tagsKey, a.metricMap.Counters)
		} else {
			a.metricMap.Counters[key][tagsKey] = gostatsd.Counter{
				Timestamp: counter.Timestamp,
				Source:    counter.Source,
				Tags:      counter.Tags,
			}
		}
	})

	a.metricMap.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		if isExpired(a.expiryIntervalTimer, nowNano, timer.Timestamp) {
			deleteMetric(key, tagsKey, a.metricMap.Timers)
		} else {
			if hasHistogramTag(timer) {
				a.metricMap.Timers[key][tagsKey] = gostatsd.Timer{
					Timestamp: timer.Timestamp,
					Source:    timer.Source,
					Tags:      timer.Tags,
					Values:    timer.Values[:0],
					Histogram: emptyHistogram(timer, a.histogramLimit),
				}
			} else {
				a.metricMap.Timers[key][tagsKey] = gostatsd.Timer{
					Timestamp: timer.Timestamp,
					Source:    timer.Source,
					Tags:      timer.Tags,
					Values:    timer.Values[:0],
				}
			}
		}
	})

	a.metricMap.Gauges.Each(func(key, tagsKey string, gauge gostatsd.Gauge) {
		if isExpired(a.expiryIntervalGauge, nowNano, gauge.Timestamp) {
			deleteMetric(key, tagsKey, a.metricMap.Gauges)
		}
		// No reset for gauges, they keep the last value until expiration
	})

	a.metricMap.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		if isExpired(a.expiryIntervalSet, nowNano, set.Timestamp) {
			deleteMetric(key, tagsKey, a.metricMap.Sets)
		} else {
			a.metricMap.Sets[key][tagsKey] = gostatsd.Set{
				Values:    make(map[string]struct{}),
				Timestamp: set.Timestamp,
				Source:    set.Source,
				Tags:      set.Tags,
			}
		}
	})
}

// ReceiveMap takes a single metric map and will aggregate the values
func (a *MetricAggregator) ReceiveMap(mm *gostatsd.MetricMap) {
	a.metricMapsReceived++
	a.metricMap.Merge(mm)
}
