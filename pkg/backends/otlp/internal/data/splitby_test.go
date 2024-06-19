package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1common "go.opentelemetry.io/proto/otlp/common/v1"
)

func TestSplitTagsByKeys(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name      string
		tags      []string
		keys      []string
		matched   []string
		unmatched []string
	}{
		{
			"no keys",
			[]string{
				"service.name:my-awesome-service",
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			},
			[]string{},
			[]string{},
			[]string{
				"service.name:my-awesome-service",
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			},
		},
		{
			"no keys match",
			[]string{
				"service.name:my-awesome-service",
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			},
			[]string{
				"service",
			},
			[]string{},
			[]string{
				"service.name:my-awesome-service",
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			},
		},
		{
			"matched end value",
			[]string{
				"service.name:my-awesome-service",
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			},
			[]string{
				"service.region",
			},
			[]string{
				"service.region:home",
			},
			[]string{
				"service.version:v1.0.0",
				"service.environment:local",
				"service.name:my-awesome-service",
			},
		},
		{
			"matched start value",
			[]string{
				"service.name:my-awesome-service",
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			},
			[]string{
				"service.name",
			},
			[]string{
				"service.name:my-awesome-service",
			},
			[]string{
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			actualMatched, actualUnmatched := splitTagsByKeys(tc.tags, tc.keys)

			assert.Equal(t, tc.matched, actualMatched)
			assert.Equal(t, tc.unmatched, actualUnmatched)
		})
	}
}

func TestSplitMetricTagsByKeys(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		tags      []string
		keys      []string
		matched   Map
		unmatched Map
	}{
		{
			name: "no keys",
			tags: []string{
				"service.name:my-awesome-service",
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			},
			matched: NewMap(),
			unmatched: NewMap(WithStatsdDelimitedTags([]string{
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

			matched: NewMap(),
			unmatched: NewMap(WithStatsdDelimitedTags([]string{
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
			matched: NewMap(WithStatsdDelimitedTags([]string{
				"service.region:home",
			})),
			unmatched: NewMap(WithStatsdDelimitedTags([]string{
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
			matched: NewMap(WithStatsdDelimitedTags([]string{
				"service.name:my-awesome-service",
			})),
			unmatched: NewMap(WithStatsdDelimitedTags([]string{
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			})),
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			matched, unmatched := SplitMetricTagsByKeysAndConvert(tc.tags, tc.keys)

			assert.Equal(t, tc.matched, matched, "Must match the matched values")
			assert.Equal(t, tc.unmatched, unmatched, "Must match the unmatched values")
		})
	}
}

func TestSplitEventTagsByKeys(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name      string
		tags      []string
		keys      []string
		matched   Map
		unmatched Map
	}{
		{
			name: "no keys",
			tags: []string{
				"service.name:my-awesome-service",
				"service.version:v1.0.0",
				"service.environment:local",
				"service.region:home",
			},
			matched: Map{raw: &[]*v1common.KeyValue{}},
			unmatched: Map{
				raw: &[]*v1common.KeyValue{
					{
						Key:   "service_environment",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "local"}},
					},
					{
						Key:   "service_name",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "my-awesome-service"}},
					},
					{
						Key:   "service_region",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "home"}},
					},
					{
						Key:   "service_version",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "v1.0.0"}},
					},
				},
			},
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
			matched: Map{raw: &[]*v1common.KeyValue{}},
			unmatched: Map{
				raw: &[]*v1common.KeyValue{
					{
						Key:   "service_environment",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "local"}},
					},
					{
						Key:   "service_name",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "my-awesome-service"}},
					},
					{
						Key:   "service_region",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "home"}},
					},
					{
						Key:   "service_version",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "v1.0.0"}},
					},
				},
			},
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
			matched: Map{
				raw: &[]*v1common.KeyValue{
					{
						Key:   "service_region",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "home"}},
					},
				},
			},
			unmatched: Map{
				raw: &[]*v1common.KeyValue{
					{
						Key:   "service_environment",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "local"}},
					},
					{
						Key:   "service_name",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "my-awesome-service"}},
					},
					{
						Key:   "service_version",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "v1.0.0"}},
					},
				},
			},
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
			matched: Map{
				raw: &[]*v1common.KeyValue{
					{
						Key:   "service_name",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "my-awesome-service"}},
					},
				},
			},
			unmatched: Map{
				raw: &[]*v1common.KeyValue{
					{
						Key:   "service_environment",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "local"}},
					},
					{
						Key:   "service_region",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "home"}},
					},
					{
						Key:   "service_version",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "v1.0.0"}},
					},
				},
			},
		},
		{
			name: "matched multiple tags",
			tags: []string{
				"perimeter:commercial",
				"environment:prod",
				"micros.service.id:metrics-gateway",
				"service.region:home",
			},
			keys: []string{
				"perimeter",
				"environment",
			},
			matched: Map{
				raw: &[]*v1common.KeyValue{
					{
						Key:   "environment",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "prod"}},
					},
					{
						Key:   "perimeter",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "commercial"}},
					},
				},
			},
			unmatched: Map{
				raw: &[]*v1common.KeyValue{
					{
						Key:   "micros_service_id",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "metrics-gateway"}},
					},
					{
						Key:   "service_region",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "home"}},
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			actualMatched, actualUnmatched := SplitEventTagsByKeysAndConvert(tc.tags, tc.keys)

			assert.Equal(t, tc.matched.Hash(), actualMatched.Hash())
			assert.Equal(t, tc.unmatched.Hash(), actualUnmatched.Hash())
		})
	}
}
