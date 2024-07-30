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

	for _, tc := range []struct {
		name             string
		batchSize        int
		metricsAdded     map[string][]string // map[resourceMetrics][]metricNames
		wantNumOfBatches int
	}{
		{
			name:             "no metrics",
			batchSize:        2,
			metricsAdded:     map[string][]string{},
			wantNumOfBatches: 1, // Must have at least one batch
		},
		{
			name:      "metrics in one resource metrics exceeds batch limit should be split into two groups",
			batchSize: 2,
			metricsAdded: map[string][]string{
				"r1": {"m1", "m2", "m3", "m4", "m5"},
			},
			wantNumOfBatches: 3,
		},
		{
			name:      "metrics in multiple resource metrics doesn't exceeds batch limit stay in one group",
			batchSize: 10,
			metricsAdded: map[string][]string{
				"r1": {"m1", "m2", "m3"},
				"r2": {"m1", "m2", "m3"},
				"r3": {"m1", "m2", "m3"},
			},
			wantNumOfBatches: 1,
		},
		{
			name:      "should properly split metrics into groups according to batch size",
			batchSize: 5,
			metricsAdded: map[string][]string{
				"r1": {"m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9", "m10"},
				"r2": {"m1"},
				"r3": {"m1", "m2", "m3", "m4", "m5"},
				"r4": {"m1", "m2", "m3"},
			},
			wantNumOfBatches: 4,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g := NewGroup(tc.batchSize)
			is := data.NewInstrumentationScope("gostatsd/aggregation", "v1.0.0")
			for rm, metricNames := range tc.metricsAdded {
				for _, metricName := range metricNames {
					insertMetric(&g, is, rm, metricName)
				}
			}

			assert.Len(t, g.batches, tc.wantNumOfBatches, "Must have %d groups", tc.wantNumOfBatches)
		})
	}
}

func insertMetric(g *Group, is data.InstrumentationScope, serviceName string, metricName string) {
	g.Insert(
		is,
		data.NewMap(
			data.WithStatsdDelimitedTags(
				[]string{
					"service.name:" + serviceName,
					"service.region:local",
				},
			),
		),
		data.NewMetric(metricName),
	)
}
