package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGauge(t *testing.T) {
	t.Parallel()

	g := NewGauge()
	assert.Len(t, g.raw.DataPoints, 0, "Must have no datapoints defined")

	g = NewGauge(NewNumberDataPoint(), NewNumberDataPoint())
	assert.Len(t, g.raw.DataPoints, 2, "Must have two datapoints defined")
}
