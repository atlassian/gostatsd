package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1common "go.opentelemetry.io/proto/otlp/common/v1"
)

func TestNewMap(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		opts []func(Map)
		m    []*v1common.KeyValue
	}{
		{
			name: "Blank map",
			m:    nil,
		},
		{
			name: "String delim values",
			opts: []func(Map){
				WithDelimitedStrings(":",
					[]string{
						"service.name:my-awesome-service",
						"service.version:1.0.0",
						"service.namespace:internal/important:very",
					},
				),
			},
			m: []*v1common.KeyValue{
				{
					Key: "service.name",
					Value: &v1common.AnyValue{
						Value: &v1common.AnyValue_StringValue{
							StringValue: "my-awesome-service",
						},
					},
				},
				{
					Key: "service.namespace",
					Value: &v1common.AnyValue{
						Value: &v1common.AnyValue_StringValue{
							StringValue: "internal/important:very",
						},
					},
				},
				{
					Key: "service.version",
					Value: &v1common.AnyValue{
						Value: &v1common.AnyValue_StringValue{
							StringValue: "1.0.0",
						},
					},
				},
			},
		},
		{
			name: "Multiple values",
			opts: []func(Map){
				WithStatsdDelimitedTags([]string{
					"service.name:my-awesome-service",
					"service.name:very-awesome",
				}),
			},
			m: []*v1common.KeyValue{
				{
					Key: "service.name",
					Value: &v1common.AnyValue{
						Value: &v1common.AnyValue_ArrayValue{
							ArrayValue: &v1common.ArrayValue{
								Values: []*v1common.AnyValue{
									{Value: &v1common.AnyValue_StringValue{StringValue: "my-awesome-service"}},
									{Value: &v1common.AnyValue_StringValue{StringValue: "very-awesome"}},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "no delim",
			opts: []func(Map){
				WithDelimitedStrings("$", []string{"service.name:my-awesome-service"}),
			},
			m: []*v1common.KeyValue{
				{
					Key: "service.name:my-awesome-service",
					Value: &v1common.AnyValue{
						Value: &v1common.AnyValue_StringValue{
							StringValue: "",
						},
					},
				},
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := NewMap(tc.opts...)

			assert.Equal(t, tc.m, *m.raw)
		})
	}
}

func TestMapMerge(t *testing.T) {
	t.Parallel()

	empty := NewMap()
	empty.Merge(empty)

	assert.Len(t, *empty.raw, 0, "Must not have any values set")

	m := NewMap(WithDelimitedStrings(":", []string{"service.name:my-awesome-service"}))
	assert.Len(t, *m.raw, 1, "Must have one value defined")

	empty.Merge(m)
	assert.Len(t, *empty.raw, 1, "Must have values from contained")

	empty.Merge(m)
	assert.Len(t, *empty.raw, 1, "Must have values from contained")

}

func TestMapHash(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		m      Map
		expect uint64
	}{
		{
			name:   "Empty Map",
			m:      NewMap(),
			expect: 0xCBF29CE484222325,
		},
		{
			name: "Simple Map",
			m: NewMap(WithStatsdDelimitedTags([]string{
				"service.name:my-awesome-service",
				"service.region:local",
			})),
			expect: 0x5EAB88693F03223B,
		},
		{
			name: "Complex Map",
			m: NewMap(WithStatsdDelimitedTags([]string{
				"service.name:my-awesome-service",
				"service.region:local",
				"service.region:laptop",
				"service.namespace:",
			})),
			expect: 0xECEBCB4D0907451B,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.expect, tc.m.Hash(), "Must match the expected hash value")
		})
	}
}
