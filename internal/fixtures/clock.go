package fixtures

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tilinna/clock"
)

// NewAdvancingClock attaches a virtual clock to a context which advances
// at full speed (not wall speed), and a cancel function to stop it.  The
// clock also stops if the context is canceled.
func NewAdvancingClock(ctx context.Context) (context.Context, func()) {
	clck := clock.NewMock(time.Unix(1, 0))
	ctx = clock.Context(ctx, clck)
	ch := make(chan struct{})
	go func() {
		for {
			select {
			case <-ch:
				return
			case <-ctx.Done():
				return
			default:
				clck.AddNext()
			}
		}
	}()
	return ctx, func() {
		close(ch)
	}
}

// NextStep will advance the supplied clock.Mock until it moves, or the context.Context is canceled (which typically
// means it timed out in wall-time).  This is useful when testing things that exist inside goroutines, when it's not
// possible to tell when the goroutine is ready to consume mock time.
func NextStep(ctx context.Context, clck *clock.Mock) {
	for _, d := clck.AddNext(); d == 0 && ctx.Err() == nil; _, d = clck.AddNext() {
		time.Sleep(time.Nanosecond) // Allows the system to actually idle, runtime.Gosched() does not.
	}
}

// EnsureAttachedTimers will block execution until either expected number of timers are reached
// or the until duration has been reached causing this method to fail the test.
// A true result is returned when the assertion passes allow for control flow of control, otherwise
// the assertional failed and should ensure the test fails gracefully.
func EnsureAttachedTimers(tb testing.TB, clck *clock.Mock, expect int, until time.Duration) bool {
	return assert.Eventually(tb, func() bool {
		return clck.Len() == expect
	}, until, time.Millisecond)
}
