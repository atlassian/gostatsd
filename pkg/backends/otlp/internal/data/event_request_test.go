package data

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	v1log "go.opentelemetry.io/proto/otlp/logs/v1"
)

func TestEventRequest(t *testing.T) {
	t.Parallel()

	req, err := NewEventsRequest(
		context.Background(),
		"localhost:1234",
		&v1log.LogRecord{},
		NewMap(),
	)
	assert.NoError(t, err, "Must not error creating request")
	assert.NotNil(t, req, "Must have a valid request")

	body, err := io.ReadAll(req.Body)
	assert.NoError(t, err, "Must not error reading contents")
	assert.NotEmpty(t, body, "Must have content set")
}
