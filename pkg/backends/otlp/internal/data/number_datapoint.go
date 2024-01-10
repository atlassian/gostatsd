package data

import (
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type NumberDataPoint struct {
	raw *v1metrics.NumberDataPoint
}

func WithNumberDatapointTimeStamp[Timestamp interface{ ~int64 | ~uint64 }](t Timestamp) func(NumberDataPoint) {
	return func(ndp NumberDataPoint) {
		ndp.raw.TimeUnixNano = uint64(t)
	}
}

func WithNumberDataPointDelimtedTags[Tags ~[]string](tags Tags) func(NumberDataPoint) {
	return func(ndp NumberDataPoint) {
		ndp.raw.Attributes = NewMap(WithStatsdDelimitedTags(tags)).unwrap()
	}
}

func WithNumberDatapointIntValue(value int64) func(NumberDataPoint) {
	return func(ndp NumberDataPoint) {
		ndp.raw.Value = &v1metrics.NumberDataPoint_AsInt{
			AsInt: value,
		}
	}
}

func WithNumberDataPointDoubleValue(value float64) func(NumberDataPoint) {
	return func(ndp NumberDataPoint) {
		ndp.raw.Value = &v1metrics.NumberDataPoint_AsDouble{
			AsDouble: value,
		}
	}
}

func NewNumberDataPoint(opts ...func(NumberDataPoint)) NumberDataPoint {
	dp := NumberDataPoint{
		raw: &v1metrics.NumberDataPoint{},
	}

	for _, opt := range opts {
		opt(dp)
	}

	return dp
}
