package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestNewSum(t *testing.T) {
	t.Parallel()

	empty := NewSum()
	assert.Equal(
		t,
		Sum{
			raw: &v1metrics.Sum{
				IsMonotonic:            false,
				AggregationTemporality: v1metrics.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
				DataPoints:             make([]*v1metrics.NumberDataPoint, 0),
			},
		},
		empty,
	)

	s := NewSum(NewNumberDataPoint(100), NewNumberDataPoint(100))
	assert.Len(t, s.raw.DataPoints, 2, "Must have two datapoints set")
}
