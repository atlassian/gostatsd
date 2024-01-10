package data

import (
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type ScopeMetrics struct {
	raw *v1metrics.ScopeMetrics
}

func NewScopeMetrics(is InstrumentationScope, metrics ...Metric) ScopeMetrics {
	sm := ScopeMetrics{
		raw: &v1metrics.ScopeMetrics{
			Scope:   is.raw,
			Metrics: make([]*v1metrics.Metric, 0, len(metrics)),
		},
	}

	for i := 0; i < len(metrics); i++ {
		sm.raw.Metrics = append(sm.raw.Metrics, metrics[i].raw)
	}

	return sm
}
