package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewResource(t *testing.T) {
	t.Parallel()

	empty := NewResource()
	assert.Len(t, empty.raw.Attributes, 0, "Must have no values defined")

	r := NewResource(
		WithResourceAttributes(
			WithDelimitedStrings(":", []string{"service.name:my-awesome-service"}),
		),
	)
	assert.Len(t, r.raw.Attributes, 1, "must have one value defined")
}
