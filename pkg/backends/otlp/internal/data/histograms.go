package data

import (
	"math"
	"slices"

	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	"golang.org/x/exp/maps"
)

type Histogram struct {
	raw *v1metrics.Histogram
}

type HistogramDataPoint struct {
	raw *v1metrics.HistogramDataPoint
}

func NewHistogram(datapoints ...HistogramDataPoint) Histogram {
	ht := Histogram{
		raw: &v1metrics.Histogram{
			AggregationTemporality: v1metrics.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
			DataPoints:             make([]*v1metrics.HistogramDataPoint, 0, len(datapoints)),
		},
	}

	for i := 0; i < len(datapoints); i++ {
		ht.raw.DataPoints = append(
			ht.raw.DataPoints,
			datapoints[i].raw,
		)
	}

	return ht
}

func WithHistogramDataPointAttributes(attrs Map) func(HistogramDataPoint) {
	return func(hdp HistogramDataPoint) {
		hdp.raw.Attributes = attrs.unwrap()
	}
}

func WithHistogramDataPointStatistics(values []float64) func(HistogramDataPoint) {
	return func(hdp HistogramDataPoint) {
		hdp.raw.Sum = new(float64)
		hdp.raw.Min = &values[0]
		hdp.raw.Max = &values[len(values)-1]
		hdp.raw.Count = uint64(len(values))

		for _, v := range values {
			*hdp.raw.Sum += v
			*hdp.raw.Min = math.Min(*hdp.raw.Min, v)
			*hdp.raw.Max = math.Max(*hdp.raw.Max, v)
		}
	}
}

func WithHistogramDataPointBucketValues[Buckets ~map[float64]uint64](buckets Buckets) func(HistogramDataPoint) {
	return func(hdp HistogramDataPoint) {
		bounds := maps.Keys(buckets)
		slices.Sort(bounds)

		hdp.raw.BucketCounts = make([]uint64, len(buckets))
		hdp.raw.ExplicitBounds = make([]float64, len(buckets)-1)

		for i, bound := range bounds {
			hdp.raw.BucketCounts[i] = buckets[bound]
			if !math.IsInf(bound, 1) {
				hdp.raw.ExplicitBounds[i] = bound
			}
		}
	}
}

func NewHistogramDataPoint(timestamp uint64, opts ...func(HistogramDataPoint)) HistogramDataPoint {
	dp := HistogramDataPoint{
		raw: &v1metrics.HistogramDataPoint{
			TimeUnixNano: timestamp,
		},
	}

	for i := 0; i < len(opts); i++ {
		opts[i](dp)
	}

	return dp
}
