package data

import (
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type Gauge struct {
	embed[*v1metrics.Gauge]
}

func NewGauge(datapoints ...NumberDataPoint) Gauge {
	g := Gauge{
		embed: newEmbed[*v1metrics.Gauge](
			func(e embed[*v1metrics.Gauge]) {
				e.t.DataPoints = make([]*v1metrics.NumberDataPoint, 0, len(datapoints))
			},
		),
	}

	for i := 0; i < len(datapoints); i++ {
		g.embed.t.DataPoints = append(g.embed.t.DataPoints, datapoints[i].AsRaw())
	}

	return g
}
