package stats

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChangeGaugeSendIfChanged(t *testing.T) {
	cg := &ChangeGauge{}
	cs := &countingStatser{}

	cg.SendIfChanged(cs, "change", nil)
	require.Zero(t, cs.gauges)
	cg.Cur = 5
	for i := 1; i <= repeatCount; i++ {
		cg.SendIfChanged(cs, "change", nil)
		require.EqualValues(t, i, cs.gauges)
	}
	cg.SendIfChanged(cs, "change", nil) // should not send
	require.EqualValues(t, repeatCount, cs.gauges)
}
