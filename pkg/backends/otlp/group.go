package otlp

import (
	"golang.org/x/exp/maps"

	"github.com/atlassian/gostatsd/pkg/backends/otlp/internal/data"
)

// Group is used to ensure metrics that have the same resource attributes
// are grouped together and it uses a fixed values to reduce potential memory
// allocations compared to using a string value
type Group map[uint64]data.ResourceMetrics

func NewGroup() Group {
	return make(Group)
}

func (g *Group) Values() []data.ResourceMetrics {
	return maps.Values(*g)
}

func (g *Group) Insert(is data.InstrumentationScope, resources data.Map, m data.Metric) {
	key := resources.Hash()

	entry, exist := (*g)[key]
	if !exist {
		entry = data.NewResourceMetrics(
			data.NewResource(
				data.WithResourceMap(resources),
			),
		)
		(*g)[key] = entry
	}

	entry.AppendMetric(is, m)
}
