package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1common "go.opentelemetry.io/proto/otlp/common/v1"
)

func TestInstrumentationScope(t *testing.T) {
	t.Parallel()

	is := NewInstrumentationScope("gostatsd", "test")
	assert.Equal(
		t,
		is,
		InstrumentationScope{
			raw: &v1common.InstrumentationScope{
				Name:    "gostatsd",
				Version: "test",
			},
		},
	)

	is = NewInstrumentationScope("gostatsd", "test",
		WithInstrumentationScopeAttributes(
			NewMap(WithDelimitedStrings(":", []string{"service.name:my-awesome-service"})),
		),
	)
	assert.Equal(
		t,
		is,
		InstrumentationScope{
			raw: &v1common.InstrumentationScope{
				Name:    "gostatsd",
				Version: "test",
				Attributes: []*v1common.KeyValue{
					{
						Key: "service.name",
						Value: &v1common.AnyValue{
							Value: &v1common.AnyValue_StringValue{
								StringValue: "my-awesome-service",
							},
						},
					},
				},
			},
		},
	)
}
