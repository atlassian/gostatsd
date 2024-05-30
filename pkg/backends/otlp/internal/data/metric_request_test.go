package data

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricsRequest(t *testing.T) {
	t.Parallel()

	req, err := NewMetricsRequest(
		context.Background(),
		"not-a-valid-url",
		[]ResourceMetrics{NewResourceMetrics(NewResource())},
	)
	assert.NoError(t, err, "Must not error creating request")
	assert.NotNil(t, req, "Must have a valid request")

	body, err := io.ReadAll(req.Body)
	assert.NoError(t, err, "Must not error reading contents")
	assert.NotEmpty(t, body, "Must have content set")
}
