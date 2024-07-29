package data

import v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"

type ResourceMetrics struct {
	raw *v1metrics.ResourceMetrics
}

func NewResourceMetrics(resource Resource, scopeMetrics ...ScopeMetrics) ResourceMetrics {
	rm := ResourceMetrics{
		raw: &v1metrics.ResourceMetrics{
			Resource:     resource.raw,
			ScopeMetrics: make([]*v1metrics.ScopeMetrics, 0, len(scopeMetrics)),
		},
	}

	for i := 0; i < len(scopeMetrics); i++ {
		rm.raw.ScopeMetrics = append(rm.raw.ScopeMetrics, scopeMetrics[i].raw)
	}

	return rm
}

func (rm ResourceMetrics) AppendMetric(is InstrumentationScope, m Metric) {
	if len(rm.raw.ScopeMetrics) == 0 {
		rm.raw.ScopeMetrics = []*v1metrics.ScopeMetrics{
			NewScopeMetrics(is).raw,
		}
	}
	rm.raw.ScopeMetrics[0].Metrics = append(rm.raw.ScopeMetrics[0].Metrics, m.raw)
}

func (rm ResourceMetrics) CouneMetrics() int {
	c := 0
	for _, sm := range rm.raw.ScopeMetrics {
		c += len(sm.Metrics)
	}
	return c
}
