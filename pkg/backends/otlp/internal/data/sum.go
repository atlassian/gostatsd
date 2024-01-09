package data

import (
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type Sum struct {
	embed[*v1metrics.Sum]
}

func NewSum(datapoints ...NumberDataPoint) Sum {
	s := Sum{
		embed: newEmbed[*v1metrics.Sum](
			func(e embed[*v1metrics.Sum]) {
				e.t.AggregationTemporality = v1metrics.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
				e.t.IsMonotonic = false
				e.t.DataPoints = make([]*v1metrics.NumberDataPoint, 0, len(datapoints))
			},
		),
	}

	for i := 0; i < len(datapoints); i++ {
		s.embed.t.DataPoints = append(s.embed.t.DataPoints, datapoints[i].AsRaw())
	}

	return s
}
