package data

import (
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type Sum struct {
	raw *v1metrics.Sum
}

func NewSum(datapoints ...NumberDataPoint) Sum {
	s := Sum{
		raw: &v1metrics.Sum{
			IsMonotonic:            false,
			AggregationTemporality: v1metrics.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
			DataPoints:             make([]*v1metrics.NumberDataPoint, 0, len(datapoints)),
		},
	}

	for i := 0; i < len(datapoints); i++ {
		s.raw.DataPoints = append(s.raw.DataPoints, datapoints[i].raw)
	}

	return s
}
