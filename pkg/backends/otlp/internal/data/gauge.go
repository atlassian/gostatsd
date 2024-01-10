package data

import (
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type Gauge struct {
	raw *v1metrics.Gauge
}

func NewGauge(datapoints ...NumberDataPoint) Gauge {
	g := Gauge{
		raw: &v1metrics.Gauge{
			DataPoints: make([]*v1metrics.NumberDataPoint, 0, len(datapoints)),
		},
	}

	for i := 0; i < len(datapoints); i++ {
		g.raw.DataPoints = append(g.raw.DataPoints, datapoints[i].raw)
	}

	return g
}
