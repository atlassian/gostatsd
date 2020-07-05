package fixtures

import (
	"context"
	"time"

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
		time.Sleep(1) // Allows the system to actually idle, runtime.Gosched() does not.
	}
}
