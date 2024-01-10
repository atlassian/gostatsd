package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1common "go.opentelemetry.io/proto/otlp/common/v1"
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestNumberDataPoint(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		opts   []func(NumberDataPoint)
		expect NumberDataPoint
	}{
		{
			name: "empty",
			expect: NumberDataPoint{
				raw: &v1metrics.NumberDataPoint{},
			},
		},
		{
			name: "int with values",
			opts: []func(NumberDataPoint){
				WithNumberDatapointTimeStamp[int64](100),
				WithNumberDataPointDelimtedTags([]string{"service.name:my-awesome-service"}),
				WithNumberDatapointIntValue(1),
			},
			expect: NumberDataPoint{
				raw: &v1metrics.NumberDataPoint{
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
					Value: &v1metrics.NumberDataPoint_AsInt{
						AsInt: 1,
					},
				},
			},
		},
		{
			name: "double with values",
			opts: []func(NumberDataPoint){
				WithNumberDatapointTimeStamp[int64](100),
				WithNumberDataPointDelimtedTags([]string{"service.name:my-awesome-service"}),
				WithNumberDataPointDoubleValue(1),
			},
			expect: NumberDataPoint{
				raw: &v1metrics.NumberDataPoint{
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
					Value: &v1metrics.NumberDataPoint_AsDouble{
						AsDouble: 1,
					},
				},
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dp := NewNumberDataPoint(tc.opts...)
			assert.Equal(t, dp, tc.expect, "Must match the expected value")
		})
	}
}
