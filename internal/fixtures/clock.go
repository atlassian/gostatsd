package fixtures

import (
	"context"
	"time"

	"github.com/tilinna/clock"
)

// NewAdvancingClock attaches a virtual clock to a context which advances
// at full speed (not wall speed), and a cancel function to stop it.
func NewAdvancingClock(ctx context.Context) (context.Context, func()) {
	clck := clock.NewMock(time.Unix(1, 0))
	ctx = clock.Context(ctx, clck)
	ch := make(chan struct{})
	go func() {
		for {
			select {
			case <-ch:
				return
			default:
				clck.AddNext()
			}
		}
	}()
	return ctx, func() {
		ch <- struct{}{}
	}
}
