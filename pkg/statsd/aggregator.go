package statsd

import (
	"context"
	"errors"
	"github.com/emirpasic/gods/maps/treemap"
	"math"
	"math/bits"
	"sort"
	"strconv"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"
)

const InfinityBucketSize int = (1<<bits.UintSize)/2 - 1
const PercentileBucketsMarkerTag = "percentiles:true"
const PercentileBucketsPow2Algorithm = "buckets:pow2"
const PercentileBucketsPow4Algorithm = "buckets:pow4"
const PercentileBucketsLinearAlgorithm = "buckets:linear"
const PercentileBucketsTailLatencyAlgorithm = "buckets:tail-latency"
const PercentileBucketsRoundDoublingAlgorithm = "buckets:round-doubling"

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
	metricsReceived   uint64
	expiryInterval    time.Duration // How often to expire metrics
	percentThresholds map[float64]percentStruct
	now               func() time.Time // Returns current time. Useful for testing.
	statser           stats.Statser
	disabledSubtypes  gostatsd.TimerSubtypes
	metricMap         *gostatsd.MetricMap
}

// NewMetricAggregator creates a new MetricAggregator object.
func NewMetricAggregator(percentThresholds []float64, expiryInterval time.Duration, disabled gostatsd.TimerSubtypes) *MetricAggregator {
	a := MetricAggregator{
		expiryInterval:    expiryInterval,
		percentThresholds: make(map[float64]percentStruct, len(percentThresholds)),
		now:               time.Now,
		statser:           stats.NewNullStatser(), // Will probably be replaced via RunMetrics
		metricMap:         gostatsd.NewMetricMap(),
		disabledSubtypes:  disabled,
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
	a.statser.Gauge("aggregator.metrics_received", float64(a.metricsReceived), nil)

	flushInSeconds := float64(flushInterval) / float64(time.Second)

	a.metricMap.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		counter.PerSecond = float64(counter.Value) / flushInSeconds
		a.metricMap.Counters[key][tagsKey] = counter
	})

	a.metricMap.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
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

			if contains(timer.Tags, PercentileBucketsMarkerTag) {
				bucketsSet, err := createBucketsSet(timer.Tags)
				if err == nil {
					timer.Buckets = calculatePercentileBuckets(timer, bucketsSet)
				}
			}

			a.metricMap.Timers[key][tagsKey] = timer
		} else {
			timer.Count = 0
			timer.SampledCount = 0
		}
	})
}

func createBucketsSet(tags gostatsd.Tags) (buckets []int, err error) {
	if contains(tags, PercentileBucketsPow2Algorithm) {
		return []int{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192}, nil
	} else if contains(tags, PercentileBucketsPow4Algorithm) {
		return []int{4, 16, 64, 256, 1024, 4096, 16384}, nil
	} else if contains(tags, PercentileBucketsLinearAlgorithm) {
		return []int{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000}, nil
	} else if contains(tags, PercentileBucketsTailLatencyAlgorithm) {
		return []int{2, 8, 32, 64, 100, 200, 300, 400, 500, 750, 1000, 1250, 1500, 1750, 2000, 4000, 6000, 8000, 10000, 20000, 30000, 40000, 50000, 60000}, nil
	} else if contains(tags, PercentileBucketsRoundDoublingAlgorithm) {
		return []int{10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000}, nil
	} else {
		return nil, errors.New("no buckets set algorithm specified")
	}
}

func calculatePercentileBuckets(timer gostatsd.Timer, bucketsSet []int) map[int]int {
	var buckets = append(bucketsSet, InfinityBucketSize)

	var bucketToCountMap = *treemap.NewWithIntComparator()
	for _, bucket := range buckets {
		bucketToCountMap.Put(bucket, 0)
	}

	for _, floatValue := range timer.Values {
		var timingDatapoint = int(floatValue)

		targetBucket, currentCount := bucketToCountMap.Ceiling(timingDatapoint)

		// If the request time we've received hits the bucket exactly, we want it to be counted as the next bucket.
		if targetBucket == timingDatapoint {
			targetBucket, currentCount = bucketToCountMap.Ceiling(timingDatapoint + 1)
		}

		bucketToCountMap.Put(targetBucket, currentCount.(int)+1)
	}

	var result = make(map[int]int)

	bucketToCountMap.Each(func(key interface{}, value interface{}) {
		result[key.(int)] = value.(int)
	})

	return result
}

func (a *MetricAggregator) RunMetrics(ctx context.Context, statser stats.Statser) {
	a.statser = statser
}

func (a *MetricAggregator) Process(f ProcessFunc) {
	f(a.metricMap)
}

func (a *MetricAggregator) isExpired(now, ts gostatsd.Nanotime) bool {
	return a.expiryInterval != 0 && time.Duration(now-ts) > a.expiryInterval
}

func deleteMetric(key, tagsKey string, metrics gostatsd.AggregatedMetrics) {
	metrics.DeleteChild(key, tagsKey)
	if !metrics.HasChildren(key) {
		metrics.Delete(key)
	}
}

// Reset clears the contents of a MetricAggregator.
func (a *MetricAggregator) Reset() {
	a.metricsReceived = 0
	nowNano := gostatsd.Nanotime(a.now().UnixNano())

	a.metricMap.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		if a.isExpired(nowNano, counter.Timestamp) {
			deleteMetric(key, tagsKey, a.metricMap.Counters)
		} else {
			a.metricMap.Counters[key][tagsKey] = gostatsd.Counter{
				Timestamp: counter.Timestamp,
				Hostname:  counter.Hostname,
				Tags:      counter.Tags,
			}
		}
	})

	a.metricMap.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		if a.isExpired(nowNano, timer.Timestamp) {
			deleteMetric(key, tagsKey, a.metricMap.Timers)
		} else {
			a.metricMap.Timers[key][tagsKey] = gostatsd.Timer{
				Timestamp: timer.Timestamp,
				Hostname:  timer.Hostname,
				Tags:      timer.Tags,
				Values:    timer.Values[:0],
			}
		}
	})

	a.metricMap.Gauges.Each(func(key, tagsKey string, gauge gostatsd.Gauge) {
		if a.isExpired(nowNano, gauge.Timestamp) {
			deleteMetric(key, tagsKey, a.metricMap.Gauges)
		}
		// No reset for gauges, they keep the last value until expiration
	})

	a.metricMap.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		if a.isExpired(nowNano, set.Timestamp) {
			deleteMetric(key, tagsKey, a.metricMap.Sets)
		} else {
			a.metricMap.Sets[key][tagsKey] = gostatsd.Set{
				Values:    make(map[string]struct{}),
				Timestamp: set.Timestamp,
				Hostname:  set.Hostname,
				Tags:      set.Tags,
			}
		}
	})
}

// Receive aggregates an incoming metric.
func (a *MetricAggregator) Receive(m *gostatsd.Metric, now time.Time) {
	a.metricsReceived++
	a.metricMap.Receive(m, now)
}

// Contains tells whether a contains x.
func contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}
