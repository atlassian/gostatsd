package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMap(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		opts []func(Map)
		m    Map
	}{
		{
			name: "Blank map",
			m:    NewMap(),
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
			m: NewMap(func(m Map) {
				m.Insert("service.name", "my-awesome-service")
				m.Insert("service.version", "1.0.0")
				m.Insert("service.namespace", "internal/important:very")
			}),
		},
		{
			name: "Duplicate entries",
			opts: []func(Map){
				WithStatsdDelimitedTags([]string{
					"service.name:my-awesome-service",
					"service.name:very-awesome",
				}),
			},
			m: NewMap(func(m Map) {
				m.Insert("service.name", "my-awesome-service")
				m.Insert("service.name", "very-awesome")
			}),
		},
		{
			name: "no delim",
			opts: []func(Map){
				WithDelimitedStrings("$", []string{"service.name:my-awesome-service"}),
			},
			m: NewMap(func(m Map) {
				m.Insert("service.name:my-awesome-service", "")
			}),
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := NewMap(tc.opts...)

			if !assert.True(t, tc.m.Equal(m), "Must match the expected values") {
				assert.Equal(t, tc.m, m)
			}
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
