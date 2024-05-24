package otlp

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/atlassian/gostatsd/pkg/backends/otlp/internal/data"
)

func TestSplitTagsByKeys(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		tags      []string
		keys      []string
		matched   data.Map
		unmatched data.Map
	}{
		{
			name: "no keys",
			tags: []string{
				"service.name:my-awesome-service",
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			},
			matched: data.NewMap(),
			unmatched: data.NewMap(data.WithStatsdDelimitedTags([]string{
				"service.name:my-awesome-service",
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			})),
		},
		{
			name: "no keys match",
			tags: []string{
				"service.name:my-awesome-service",
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			},
			keys: []string{
				"service",
			},

			matched: data.NewMap(),
			unmatched: data.NewMap(data.WithStatsdDelimitedTags([]string{
				"service.name:my-awesome-service",
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			})),
		},
		{
			name: "matched end value",
			tags: []string{
				"service.name:my-awesome-service",
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			},
			keys: []string{
				"service.region",
			},
			matched: data.NewMap(data.WithStatsdDelimitedTags([]string{
				"service.region:home",
			})),
			unmatched: data.NewMap(data.WithStatsdDelimitedTags([]string{
				"service.version:v1.0.0",
				"service.environment:local",
				"service.name:my-awesome-service",
			})),
		},
		{
			name: "matched start value",
			tags: []string{
				"service.name:my-awesome-service",
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			},
			keys: []string{
				"service.name",
			},
			matched: data.NewMap(data.WithStatsdDelimitedTags([]string{
				"service.name:my-awesome-service",
			})),
			unmatched: data.NewMap(data.WithStatsdDelimitedTags([]string{
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			})),
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			matched, unmatched := splitTagsByKeys(tc.tags, tc.keys)

			assert.Equal(t, tc.matched, matched, "Must match the matched values")
			assert.Equal(t, tc.unmatched, unmatched, "Must match the unmatched values")
		})
	}
}
