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
					"service.name:my-awesome-service",
					"service.version:1.0.0",
					"service.namespace:internal/important:very",
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
				WithDelimitedStrings(":",
					"service.name:my-awesome-service",
					"service.name:very-awesome",
				),
			},
			m: NewMap(func(m Map) {
				m.Insert("service.name", "my-awesome-service")
				m.Insert("service.name", "very-awesome")
			}),
		},
		{
			name: "no delim",
			opts: []func(Map){
				WithDelimitedStrings("$", "service.name:my-awesome-service"),
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
