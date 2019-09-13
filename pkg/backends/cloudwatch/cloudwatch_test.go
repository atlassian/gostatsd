package cloudwatch

import (
	"context"
	"math"
	"testing"

	"github.com/atlassian/gostatsd"

	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockedCloudwatch struct {
	cloudwatchiface.CloudWatchAPI

	PutMetricDataHandler func(*cloudwatch.PutMetricDataInput) (*cloudwatch.PutMetricDataOutput, error)
}

func (m *mockedCloudwatch) PutMetricData(input *cloudwatch.PutMetricDataInput) (*cloudwatch.PutMetricDataOutput, error) {
	return m.PutMetricDataHandler(input)
}

func TestSendMetrics(t *testing.T) {
	t.Parallel()

	cli, err := NewClient("ns", gostatsd.TimerSubtypes{})
	require.NoError(t, err)

	expected := []struct {
		Name  string
		Unit  string
		Value float64
	}{
		{Name: "stats.counter.c1.count", Unit: "Count", Value: 5},
		{Name: "stats.counter.c1.per_second", Unit: "Count/Second", Value: 1.1},

		{Name: "stats.timers.t1.lower", Unit: "Milliseconds", Value: 0},
		{Name: "stats.timers.t1.upper", Unit: "Milliseconds", Value: 1},
		{Name: "stats.timers.t1.count", Unit: "Count", Value: 1},
		{Name: "stats.timers.t1.count_ps", Unit: "Count/Second", Value: 1.1},
		{Name: "stats.timers.t1.mean", Unit: "Milliseconds", Value: 0.5},
		{Name: "stats.timers.t1.median", Unit: "Milliseconds", Value: 0.5},
		{Name: "stats.timers.t1.std", Unit: "Milliseconds", Value: 0.1},
		{Name: "stats.timers.t1.sum", Unit: "Milliseconds", Value: 1},
		{Name: "stats.timers.t1.sum_squares", Unit: "Milliseconds", Value: 1},
		{Name: "stats.timers.t1.count_90", Unit: "Milliseconds", Value: 0.1},

		{Name: "stats.gauge.g1", Unit: "None", Value: 3},
		{Name: "stats.set.users", Unit: "None", Value: 3},
	}

	cli.cloudwatch = &mockedCloudwatch{
		PutMetricDataHandler: func(input *cloudwatch.PutMetricDataInput) (*cloudwatch.PutMetricDataOutput, error) {
			assert.Equal(t, len(input.MetricData), 14, "Should generate 14 metric entries")

			assert.Equal(t, "ns", *input.Namespace, "Namespace should be set correctly")

			for idx, row := range expected {
				assert.Equal(t, row.Name, *input.MetricData[idx].MetricName)
				assert.Equal(t, row.Unit, *input.MetricData[idx].Unit)
				assert.Equal(t, row.Value, *input.MetricData[idx].Value)
			}

			return nil, nil
		},
	}

	res := make(chan []error, 1)
	cli.SendMetricsAsync(context.Background(), metricsOneOfEach(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.NoError(t, err)
	}

}

func TestSendMetricDimensions(t *testing.T) {
	t.Parallel()

	cli, err := NewClient("ns", gostatsd.TimerSubtypes{})
	require.NoError(t, err)

	metricMap := &gostatsd.MetricMap{
		Counters: gostatsd.Counters{
			"c1": map[string]gostatsd.Counter{
				"tag1": {
					PerSecond: 1.1,
					Value:     5,
					Timestamp: gostatsd.Nanotime(100),
					Hostname:  "h1",
					Tags:      gostatsd.Tags{"tag1", "tag2:value2"},
				},
			},
		},
	}

	cli.cloudwatch = &mockedCloudwatch{
		PutMetricDataHandler: func(input *cloudwatch.PutMetricDataInput) (*cloudwatch.PutMetricDataOutput, error) {
			assert.Equal(t, len(input.MetricData), 2, "Should generate 14 metric entries")

			assert.Equal(t, "ns", *input.Namespace, "Namespace should be set correctly")

			assert.Equal(t, "stats.counter.c1.count", *input.MetricData[0].MetricName)
			assert.Equal(t, "stats.counter.c1.per_second", *input.MetricData[1].MetricName)

			assert.Equal(t, 2, len(input.MetricData[0].Dimensions))

			for i := 0; i < 2; i++ {
				assert.Equal(t, "tag1", *input.MetricData[i].Dimensions[0].Name)
				assert.Equal(t, "set", *input.MetricData[i].Dimensions[0].Value)
				assert.Equal(t, "tag2", *input.MetricData[i].Dimensions[1].Name)
				assert.Equal(t, "value2", *input.MetricData[i].Dimensions[1].Value)
			}

			return nil, nil
		},
	}

	res := make(chan []error, 1)
	cli.SendMetricsAsync(context.Background(), metricMap, func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.NoError(t, err)
	}

}

// nolint:dupl
func metricsOneOfEach() *gostatsd.MetricMap {
	return &gostatsd.MetricMap{
		Counters: gostatsd.Counters{
			"c1": map[string]gostatsd.Counter{
				"tag1": {PerSecond: 1.1, Value: 5, Timestamp: gostatsd.Nanotime(100), Hostname: "h1", Tags: gostatsd.Tags{"tag1"}},
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
	}
}

func TestSendHistogram(t *testing.T) {
	t.Parallel()

	cli, err := NewClient("ns", gostatsd.TimerSubtypes{})
	require.NoError(t, err)

	metricMap := &gostatsd.MetricMap{
		Timers: gostatsd.Timers{
			"t1": map[string]gostatsd.Timer{
				"tag2": {
					Values:    []float64{0, 1},
					Timestamp: gostatsd.Nanotime(200),
					Hostname:  "h2",
					Tags:      gostatsd.Tags{"tag2", "gsd_histogram:20_30_40_50_60"},
					Histogram: map[gostatsd.HistogramThreshold]int{
						20:                                       5,
						30:                                       10,
						40:                                       10,
						50:                                       10,
						60:                                       19,
						gostatsd.HistogramThreshold(math.Inf(1)): 19,
					},
				},
			},
		},
	}

	cli.cloudwatch = &mockedCloudwatch{
		PutMetricDataHandler: func(input *cloudwatch.PutMetricDataInput) (*cloudwatch.PutMetricDataOutput, error) {
			assert.Equal(t, *findMetricDatum(input, "stats.timers.t1.histogram", "le", "20").Value, float64(5))
			assert.Equal(t, *findMetricDatum(input, "stats.timers.t1.histogram", "le", "30").Value, float64(10))
			assert.Equal(t, *findMetricDatum(input, "stats.timers.t1.histogram", "le", "40").Value, float64(10))
			assert.Equal(t, *findMetricDatum(input, "stats.timers.t1.histogram", "le", "50").Value, float64(10))
			assert.Equal(t, *findMetricDatum(input, "stats.timers.t1.histogram", "le", "60").Value, float64(19))
			assert.Equal(t, *findMetricDatum(input, "stats.timers.t1.histogram", "le", "+Inf").Value, float64(19))
			assert.Len(t, input.MetricData, 6)
			return nil, nil
		},
	}

	res := make(chan []error, 1)
	cli.SendMetricsAsync(context.Background(), metricMap, func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.NoError(t, err)
	}
}

func findMetricDatum(input *cloudwatch.PutMetricDataInput, name string, dimenensionName string, dimensionValue string) *cloudwatch.MetricDatum {
	for _, data := range input.MetricData {
		MetricName := *data.MetricName
		if MetricName != name {
			continue
		}
		for _, dimension := range data.Dimensions {
			if *dimension.Name == dimenensionName && *dimension.Value == dimensionValue {
				return data
			}
		}
	}
	return nil
}
