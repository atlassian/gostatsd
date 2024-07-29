package otlp

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/atlassian/gostatsd/pkg/backends/otlp/internal/data"
)

func TestGroupInsert(t *testing.T) {
	t.Parallel()

	g := NewGroup(1000)

	is := data.NewInstrumentationScope("gostatsd/aggregation", "v1.0.0")

	g.Insert(
		is,
		data.NewMap(
			data.WithStatsdDelimitedTags(
				[]string{
					"service.name:my-awesome-service",
					"service.region:local",
				},
			),
		),
		data.NewMetric("my-metric"),
	)
	g.Insert(
		is,
		data.NewMap(
			data.WithStatsdDelimitedTags(
				[]string{
					"service.name:my-awesome-service",
					"service.region:local",
				},
			),
		),
		data.NewMetric("my-metric"),
	)
	g.Insert(
		is,
		data.NewMap(
			data.WithStatsdDelimitedTags(
				[]string{
					"service.name:my-other-service",
					"service.region:local",
				},
			),
		),
		data.NewMetric("my-metric"),
	)

	assert.Len(t, g.batches[0].Values(), 2, "Must have two distinct value")
}

func TestGroupBatch(t *testing.T) {
	t.Parallel()

	g := NewGroup(2)

	is := data.NewInstrumentationScope("gostatsd/aggregation", "v1.0.0")

	g.Insert(
		is,
		data.NewMap(
			data.WithStatsdDelimitedTags(
				[]string{
					"service.name:my-awesome-service",
					"service.region:local",
				},
			),
		),
		data.NewMetric("my-metric"),
	)
	g.Insert(
		is,
		data.NewMap(
			data.WithStatsdDelimitedTags(
				[]string{
					"service.name:my-awesome-service",
					"service.region:local",
				},
			),
		),
		data.NewMetric("my-metric"),
	)
	g.Insert(
		is,
		data.NewMap(
			data.WithStatsdDelimitedTags(
				[]string{
					"service.name:my-other-service",
					"service.region:local",
				},
			),
		),
		data.NewMetric("my-metric"),
	)

	batches := g.batches
	assert.Equal(t, 2, len(batches))
}
