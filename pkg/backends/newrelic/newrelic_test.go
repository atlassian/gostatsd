package newrelic

import (
	"testing"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreparePayload(t *testing.T) {
	t.Parallel()

	cli, err := NewClient("localhost:8001", "StatsD", "", "metric_name", "metric_type",
		"metric_per_second", "metric_value", "samples_count", "samples_min", "samples_max",
		"samples_mean", "samples_median", "samples_std_dev", "samples_sum", "samples_sum_squares", gostatsd.TimerSubtypes{})
	require.NoError(t, err)

	expectedMetricSets := []Metric{
		{
			EventType: "StatsD",
			Type:      "gauge",
			Timestamp: time.Now().Unix(),
			Value:     3,
		},
		{
			EventType: "StatsD",
			Type:      "set",
			Timestamp: time.Now().Unix(),
			Value:     3,
		},
		{
			EventType: "StatsD",
			Type:      "counter",
			Timestamp: time.Now().Unix(),
			PerSecond: 1.1,
			Value:     5,
		},
		{
			EventType:  "StatsD",
			Type:       "timer",
			Timestamp:  time.Now().Unix(),
			Value:      0,
			Count:      1,
			PerSecond:  1.1,
			Mean:       0.5,
			Median:     0.5,
			Min:        0,
			Max:        1,
			StdDev:     0.1,
			Sum:        1,
			SumSquares: 1,
		},
	}

	statsdMetrics := cli.preparePayload(metricsOneOfEach(), &cli.disabledSubtypes)
	assert.Equal(t, len(expectedMetricSets), len(statsdMetrics), "Should generate 4 metric sets")

	for no, metric := range expectedMetricSets {
		assert.Equal(t, metric.EventType, statsdMetrics[no].EventType)
		assert.Equal(t, metric.Type, statsdMetrics[no].Type)
		assert.Equal(t, metric.Value, statsdMetrics[no].Value)
		assert.Equal(t, metric.Count, statsdMetrics[no].Count)
		assert.Equal(t, metric.PerSecond, statsdMetrics[no].PerSecond)
		assert.Equal(t, metric.Mean, statsdMetrics[no].Mean)
		assert.Equal(t, metric.Median, statsdMetrics[no].Median)
		assert.Equal(t, metric.Min, statsdMetrics[no].Min)
		assert.Equal(t, metric.Max, statsdMetrics[no].Max)
		assert.Equal(t, metric.StdDev, statsdMetrics[no].StdDev)
		assert.Equal(t, metric.Sum, statsdMetrics[no].Sum)
		assert.Equal(t, metric.SumSquares, statsdMetrics[no].SumSquares)
	}

}

func metricsOneOfEach() *gostatsd.MetricMap {
	return &gostatsd.MetricMap{
		Gauges: gostatsd.Gauges{
			"g1": map[string]gostatsd.Gauge{
				"tag3": {Value: 3, Timestamp: gostatsd.Nanotime(300), Hostname: "h3", Tags: gostatsd.Tags{"tag3"}},
			},
		},
		Sets: gostatsd.Sets{
			"users": map[string]gostatsd.Set{
				"tag4": {
					Values: map[string]struct{}{
						"joe":  {},
						"bob":  {},
						"john": {},
					},
					Timestamp: gostatsd.Nanotime(400),
					Hostname:  "h4",
					Tags:      gostatsd.Tags{"tag4"},
				},
			},
		},
		Timers: gostatsd.Timers{
			"t1": map[string]gostatsd.Timer{
				"tag2": {
					Count:      1,
					PerSecond:  1.1,
					Mean:       0.5,
					Median:     0.5,
					Min:        0,
					Max:        1,
					StdDev:     0.1,
					Sum:        1,
					SumSquares: 1,
					Values:     []float64{0, 1},
					Percentiles: gostatsd.Percentiles{
						gostatsd.Percentile{Float: 0.1, Str: "count_90"},
					},
					Timestamp: gostatsd.Nanotime(200),
					Hostname:  "h2",
					Tags:      gostatsd.Tags{"tag2"},
				},
			},
		},
		Counters: gostatsd.Counters{
			"c1": map[string]gostatsd.Counter{
				"tag1": {PerSecond: 1.1, Value: 5, Timestamp: gostatsd.Nanotime(100), Hostname: "h1", Tags: gostatsd.Tags{"tag1"}},
			},
		},
	}
}
