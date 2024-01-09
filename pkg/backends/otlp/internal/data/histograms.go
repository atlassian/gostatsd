package data

import (
	"math"
	"slices"

	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	"golang.org/x/exp/maps"
)

type Histogram struct {
	embed[*v1metrics.Histogram]
}

type HistogramDataPoint struct {
	embed[*v1metrics.HistogramDataPoint]
}

func NewHistogramMetric(datapoints ...HistogramDataPoint) Histogram {
	ht := Histogram{
		embed: newEmbed[*v1metrics.Histogram](
			func(e embed[*v1metrics.Histogram]) {
				e.t.AggregationTemporality = v1metrics.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
			},
		),
	}

	for i := 0; i < len(datapoints); i++ {
		ht.embed.t.DataPoints = append(
			ht.embed.t.DataPoints,
			datapoints[i].AsRaw(),
		)
	}

	return ht
}

func WithHistogramDataPointTimeStamp(ts int64) func(HistogramDataPoint) {
	return func(hdp HistogramDataPoint) {
		hdp.embed.t.TimeUnixNano = uint64(ts)
	}
}

func WithHistogramDataPointAttributes(attrs Map) func(HistogramDataPoint) {
	return func(hdp HistogramDataPoint) {
		hdp.embed.t.Attributes = attrs.unwrap()
	}
}

func WithHistogramDataPointStatistics(values []float64) func(HistogramDataPoint) {
	return func(hdp HistogramDataPoint) {
		hdp.t.Sum = new(float64)
		hdp.t.Min = &values[0]
		hdp.t.Max = &values[len(values)-1]
		hdp.t.Count = uint64(len(values))

		for _, v := range values {
			*hdp.t.Sum += v
			*hdp.t.Min = math.Min(*hdp.t.Min, v)
			*hdp.t.Max = math.Max(*hdp.t.Max, v)
		}
	}
}

func WithHistogramDataPointBucketValues[Buckets ~map[float64]uint64](buckets Buckets) func(HistogramDataPoint) {
	return func(hdp HistogramDataPoint) {
		bounds := maps.Keys(buckets)
		slices.Sort(bounds)

		hdp.embed.t.BucketCounts = make([]uint64, len(buckets))
		hdp.embed.t.ExplicitBounds = make([]float64, len(buckets)-1)

		for i, bound := range bounds {
			hdp.embed.t.BucketCounts[i] = buckets[bound]
			if !math.IsInf(bound, 1) {
				hdp.embed.t.ExplicitBounds[i] = bound
			}
		}
	}
}

func NewHistogramDataPoint(opts ...func(HistogramDataPoint)) HistogramDataPoint {
	dp := HistogramDataPoint{
		embed: newEmbed[*v1metrics.HistogramDataPoint](),
	}

	for i := 0; i < len(opts); i++ {
		opts[i](dp)
	}

	return dp
}
