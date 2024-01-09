package data

import (
	"github.com/atlassian/gostatsd"
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type NumberDataPoint struct {
	embed[*v1metrics.NumberDataPoint]
}

func WithNumberDatapointTimeStamp(t gostatsd.Nanotime) func(NumberDataPoint) {
	return func(ndp NumberDataPoint) {
		ndp.embed.t.TimeUnixNano = uint64(t)
	}
}

func WithNumberDataPointDelimtedTags(tags gostatsd.Tags) func(NumberDataPoint) {
	return func(ndp NumberDataPoint) {
		ndp.embed.t.Attributes = NewMap(WithStatsdDelimitedTags(tags)).unwrap()
	}
}

func WithNumberDatapointIntValue(value int64) func(NumberDataPoint) {
	return func(ndp NumberDataPoint) {
		ndp.embed.t.Value = &v1metrics.NumberDataPoint_AsInt{
			AsInt: value,
		}
	}
}

func WithNumberDataPointDoubleValue(value float64) func(NumberDataPoint) {
	return func(ndp NumberDataPoint) {
		ndp.embed.t.Value = &v1metrics.NumberDataPoint_AsDouble{
			AsDouble: value,
		}
	}
}

func NewNumberDatapoint(opts ...func(NumberDataPoint)) NumberDataPoint {
	dp := NumberDataPoint{
		embed: newEmbed[*v1metrics.NumberDataPoint](),
	}

	for _, opt := range opts {
		opt(dp)
	}

	return dp
}
