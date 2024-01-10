package data

import (
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type Metric struct {
	raw *v1metrics.Metric
}

// NewMetric creates a new metric wrapper and
// sets the metric value based on the provided function.
func NewMetric(name string) Metric {
	return Metric{
		raw: &v1metrics.Metric{
			Name: name,
		},
	}
}

func (m Metric) SetGauge(g Gauge) Metric {
	m.raw.Data = &v1metrics.Metric_Gauge{
		Gauge: g.raw,
	}
	return m
}

func (m Metric) SetSum(s Sum) Metric {
	m.raw.Data = &v1metrics.Metric_Sum{
		Sum: s.raw,
	}
	return m
}

func (m Metric) SetHistogram(h Histogram) Metric {
	m.raw.Data = &v1metrics.Metric_Histogram{
		Histogram: h.raw,
	}
	return m
}
