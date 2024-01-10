package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestNewMetric(t *testing.T) {
	t.Parallel()

	g, s, h := NewGauge(), NewSum(), NewHistogram()

	m := NewMetric("my-awesome-metric")
	assert.Equal(t, "my-awesome-metric", m.raw.Name, "Must match the expected name")
	assert.Nil(t, m.raw.Data, "No metric value set")

	m.SetGauge(g)
	assert.Equal(t, m.raw.Data, &v1metrics.Metric_Gauge{
		Gauge: g.raw,
	})

	m.SetSum(s)
	assert.Equal(t, m.raw.Data, &v1metrics.Metric_Sum{
		Sum: s.raw,
	})

	m.SetHistogram(h)
	assert.Equal(t, m.raw.Data, &v1metrics.Metric_Histogram{
		Histogram: h.raw,
	})
}
