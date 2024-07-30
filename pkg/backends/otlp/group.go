package otlp

import (
	"golang.org/x/exp/maps"

	"github.com/atlassian/gostatsd/pkg/backends/otlp/internal/data"
)

type group map[uint64]data.ResourceMetrics

func (g *group) Values() []data.ResourceMetrics {
	return maps.Values(*g)
}

func (g *group) LenMetrics() int {
	count := 0
	for _, rm := range *g {
		count += rm.CouneMetrics()
	}
	return count
}

// Groups is used to ensure metrics that have the same resource attributes
// are grouped together and it uses a fixed values to reduce potential memory
// allocations compared to using a string value
type Groups struct {
	batches         []group
	metricsInserted int
	batchSize       int
}

func NewGroups(batchSize int) Groups {
	return Groups{
		batches:   []group{make(group)},
		batchSize: batchSize,
	}
}

func (g *Groups) Insert(is data.InstrumentationScope, resources data.Map, m data.Metric) {
	key := resources.Hash()

	currentBatch := g.batches[len(g.batches)-1]
	entry, exist := (currentBatch)[key]
	if !exist {
		entry = data.NewResourceMetrics(
			data.NewResource(
				data.WithResourceMap(resources),
			),
		)
		(currentBatch)[key] = entry
	}
	entry.AppendMetric(is, m)
	currentBatch[key] = entry

	if currentBatch.LenMetrics() >= g.batchSize {
		// next insertion "current batch" will be the a new batch
		g.batches = append(g.batches, make(group))
	}
}
