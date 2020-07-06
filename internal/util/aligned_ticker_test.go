package util

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tilinna/clock"

	"github.com/atlassian/gostatsd/internal/fixtures"
)

const ms = int64(time.Millisecond)

func checkTime(t *testing.T, ctx context.Context, ch <-chan time.Time, expected time.Time) {
	select {
	case <-ctx.Done():
		require.FailNow(t, "timed out")
	case now := <-ch:
		require.Equal(t, expected.UnixNano(), now.UnixNano())
	}
}

func TestAlignedTickerSimple(t *testing.T) {
	clck := clock.NewMock(time.Unix(1, 0))
	ctx, cancel := context.WithTimeout(clock.Context(context.Background(), clck), 100*time.Millisecond)
	defer cancel()
	tckr := NewAlignedTickerWithContext(ctx, 1000*time.Millisecond, 0*time.Millisecond)

	// First update will go from 1s -> 2s
	fixtures.NextStep(ctx, clck)
	checkTime(t, ctx, tckr.C, time.Unix(2, 0))

	// Second update will go from 2s -> 3s
	fixtures.NextStep(ctx, clck)
	checkTime(t, ctx, tckr.C, time.Unix(3, 0))

	tckr.Stop()
}

func TestAlignedTickerInitialRound(t *testing.T) {
	clck := clock.NewMock(time.Unix(1, int64(500*time.Millisecond)))
	ctx, cancel := context.WithTimeout(clock.Context(context.Background(), clck), 100*time.Millisecond)
	defer cancel()
	tckr := NewAlignedTickerWithContext(ctx, 1000*time.Millisecond, 0*time.Millisecond)

	// First update will go from 1.5s to 2s
	fixtures.NextStep(ctx, clck)
	checkTime(t, ctx, tckr.C, time.Unix(2, 0))

	// Second update will go from 2s to 3s
	fixtures.NextStep(ctx, clck)
	checkTime(t, ctx, tckr.C, time.Unix(3, 0))

	tckr.Stop()
}

func TestAlignedTickerOffset(t *testing.T) {
	clck := clock.NewMock(time.Unix(1, 300*ms))
	ctx, cancel := context.WithTimeout(clock.Context(context.Background(), clck), 100*time.Millisecond)
	defer cancel()
	tckr := NewAlignedTickerWithContext(ctx, 1000*time.Millisecond, 300*time.Millisecond)

	// First update will go from 1.3s to 2.3s
	fixtures.NextStep(ctx, clck)
	checkTime(t, ctx, tckr.C, time.Unix(2, 300*ms))

	// Second update will go from 2.3s to 3.3s
	fixtures.NextStep(ctx, clck)
	checkTime(t, ctx, tckr.C, time.Unix(3, 300*ms))

	tckr.Stop()
}

func TestAlignedTickerInitialRoundOffset(t *testing.T) {
	clck := clock.NewMock(time.Unix(1, 0))
	ctx, cancel := context.WithTimeout(clock.Context(context.Background(), clck), 100*time.Millisecond)
	defer cancel()
	tckr := NewAlignedTickerWithContext(ctx, 1000*time.Millisecond, 300*time.Millisecond)

	// First update will go from 1s to 1.3s
	fixtures.NextStep(ctx, clck)
	checkTime(t, ctx, tckr.C, time.Unix(1, 300*ms))

	// Second update will go from 1.3s to 2.3s
	fixtures.NextStep(ctx, clck)
	checkTime(t, ctx, tckr.C, time.Unix(2, 300*ms))

	tckr.Stop()
}
