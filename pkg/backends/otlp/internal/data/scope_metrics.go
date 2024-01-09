package data

import (
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type ScopeMetrics struct {
	embed[*v1metrics.ScopeMetrics]
}

func NewScopeMetrics(instrumentation ...func(InstrumentationScope)) ScopeMetrics {
	return ScopeMetrics{
		embed: newEmbed[*v1metrics.ScopeMetrics](
			func(e embed[*v1metrics.ScopeMetrics]) {
				e.t.Scope = NewInstrumentationScope("gostatsd", "v0.0.0-unset", instrumentation...).AsRaw()
			},
		),
	}
}

func (sm ScopeMetrics) AppendMetrics(metrics ...Metric) {
	// TODO: Ensure there is capacity within the existing slice
	//       to copy values over
	for i := 0; i < len(metrics); i++ {
		sm.t.Metrics = append(sm.t.Metrics, metrics[i].AsRaw())
	}
}
