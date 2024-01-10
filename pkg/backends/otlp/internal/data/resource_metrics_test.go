package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1resource "go.opentelemetry.io/proto/otlp/resource/v1"
)

func TestNewResourceMetrics(t *testing.T) {
	t.Parallel()

	empty := NewResourceMetrics(NewResource())
	assert.Equal(
		t,
		&v1resource.Resource{},
		empty.raw.Resource,
	)
	assert.Len(t, empty.raw.ScopeMetrics, 0, "Must have no values set")

	rm := NewResourceMetrics(
		NewResource(),
		NewScopeMetrics(
			NewInstrumentationScope("gostatsd/aggregation", "test"),
		),
		NewScopeMetrics(
			NewInstrumentationScope("gostatsd/aggregation", "test"),
		),
	)
	assert.Len(t, rm.raw.ScopeMetrics, 2, "Must have two scope metrics defined")
}
