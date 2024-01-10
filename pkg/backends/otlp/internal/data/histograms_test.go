package data

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	v1common "go.opentelemetry.io/proto/otlp/common/v1"
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestHistogram(t *testing.T) {
	t.Parallel()

	empty := NewHistogram()
	assert.Len(t, empty.raw.DataPoints, 0, "Must have no datapoints defined")
	assert.Equal(t,
		v1metrics.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
		empty.raw.AggregationTemporality,
	)

	h := NewHistogram(NewHistogramDataPoint(), NewHistogramDataPoint())
	assert.Len(t, h.raw.DataPoints, 2, "Must have two datapoints defined")
}

func TestHistogramDataPoint(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string

		opts   []func(HistogramDataPoint)
		expect HistogramDataPoint
	}{
		{
			name: "Empty",
			expect: HistogramDataPoint{
				raw: &v1metrics.HistogramDataPoint{},
			},
		},
		{
			name: "assigns metadata",
			opts: []func(HistogramDataPoint){
				WithHistogramDataPointAttributes(
					NewMap(WithDelimitedStrings(":", []string{"service.name:my-awesome-service"})),
				),
				WithHistogramDataPointTimeStamp(100),
			},
			expect: HistogramDataPoint{
				raw: &v1metrics.HistogramDataPoint{
					TimeUnixNano: 100,
					Attributes: []*v1common.KeyValue{
						{
							Key: "service.name",
							Value: &v1common.AnyValue{
								Value: &v1common.AnyValue_StringValue{
									StringValue: "my-awesome-service",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "populates values",
			opts: []func(HistogramDataPoint){
				WithHistogramDataPointStatistics(
					[]float64{0.0, 1.2, 3.8},
				),
				WithHistogramDataPointBucketValues(map[float64]uint64{
					1.0:         1,
					1.4:         2,
					3.0:         2,
					4.0:         3,
					math.Inf(1): 3,
				}),
			},
			expect: func() HistogramDataPoint {
				min, max, sum := 0.0, 3.8, 5.0
				return HistogramDataPoint{
					raw: &v1metrics.HistogramDataPoint{
						Sum:   &sum,
						Min:   &min,
						Max:   &max,
						Count: 3,
						BucketCounts: []uint64{
							1, 2, 2, 3, 3,
						},
						ExplicitBounds: []float64{
							1.0,
							1.4,
							3.0,
							4.0,
						},
					},
				}
			}(),
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dp := NewHistogramDataPoint(tc.opts...)
			assert.Equal(t, tc.expect, dp, "Must match the expected value")
		})
	}
}
