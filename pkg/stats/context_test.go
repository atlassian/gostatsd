package stats

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContextValid(t *testing.T) {
	t.Parallel()

	originalStatser := NewLoggingStatser(nil, nil)
	ctxWithStatser := NewContext(context.Background(), originalStatser)
	returnedStatser := FromContext(ctxWithStatser)
	require.Equal(t, originalStatser, returnedStatser)
}

func TestContextInvalid(t *testing.T) {
	t.Parallel()

	returnedStatser := FromContext(context.Background())
	require.NotNil(t, returnedStatser)
}
