package data

import v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"

type ResourceMetrics struct {
	embed[*v1metrics.ResourceMetrics]
}

func NewResourceMetrics(resource Resource, scopeMetrics ...ScopeMetrics) ResourceMetrics {
	rm := ResourceMetrics{
		embed: newEmbed[*v1metrics.ResourceMetrics](
			func(e embed[*v1metrics.ResourceMetrics]) {
				e.t.Resource = resource.AsRaw()
				e.t.ScopeMetrics = make([]*v1metrics.ScopeMetrics, 0, len(scopeMetrics))
			},
		),
	}

	for i := 0; i < len(scopeMetrics); i++ {
		rm.embed.t.ScopeMetrics = append(rm.embed.t.ScopeMetrics, scopeMetrics[i].AsRaw())
	}

	return rm
}
