package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestNewScopeMetrics(t *testing.T) {
	t.Parallel()

	empty := NewScopeMetrics(
		NewInstrumentationScope("gostatsd/aggregation", "test"),
	)
	assert.Equal(
		t,
		ScopeMetrics{
			raw: &v1metrics.ScopeMetrics{
				Scope:   NewInstrumentationScope("gostatsd/aggregation", "test").raw,
				Metrics: make([]*v1metrics.Metric, 0),
			},
		},
		empty,
	)

	sm := NewScopeMetrics(
		NewInstrumentationScope("gostatsd/aggregation", "test"),
		NewMetric("my-awesome-metric"),
	)
	assert.Len(t, sm.raw.Metrics, 1, "Must have one metric set")
	assert.Equal(t, sm.raw.Metrics[0].Name, "my-awesome-metric")
}
