package data

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricsRequest(t *testing.T) {
	t.Parallel()

	req, err := NewMetricsRequest(
		context.Background(),
		"not-a-valid-url",
		NewResourceMetrics(NewResource()),
	)
	assert.NoError(t, err, "Must not error creating request")
	assert.NotNil(t, req, "Must have a valid request")
}
