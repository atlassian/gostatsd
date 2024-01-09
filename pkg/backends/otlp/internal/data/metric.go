package data

import (
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type Metric struct {
	embed[*v1metrics.Metric]
}

func WithMetricTypeGauge(gauge Gauge) func(Metric) {
	return func(m Metric) {
		m.t.Data = &v1metrics.Metric_Gauge{
			Gauge: gauge.AsRaw(),
		}
	}
}

func WithMetricTypeSum(sum Sum) func(Metric) {
	return func(m Metric) {
		m.t.Data = &v1metrics.Metric_Sum{
			Sum: sum.AsRaw(),
		}
	}
}

func WithMetricTypeHistogram(histogram Histogram) func(Metric) {
	return func(m Metric) {
		m.t.Data = &v1metrics.Metric_Histogram{
			Histogram: histogram.AsRaw(),
		}
	}
}

// NewMetric creates a new metric wrapper and
// sets the metric value based on the provided function.
func NewMetric(name string, mtype func(Metric)) Metric {
	m := Metric{
		embed: newEmbed[*v1metrics.Metric](
			func(e embed[*v1metrics.Metric]) {
				e.t.Name = name
			},
		),
	}

	mtype(m)

	return m
}
