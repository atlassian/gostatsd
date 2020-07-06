package util

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNopWriteCloser(t *testing.T) {
	require.NoError(t, NopWriteCloser(&bytes.Buffer{}).Close())
}
