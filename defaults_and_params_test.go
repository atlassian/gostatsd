package gostatsd

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestAddFlags(t *testing.T) {
	require.NotPanics(t, func() {
		fs := &pflag.FlagSet{}
		AddFlags(fs)
	})
}
