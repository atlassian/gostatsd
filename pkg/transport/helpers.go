package transport

import (
	"context"
	"io"
	"io/ioutil"
	"time"

	"github.com/tilinna/clock"
)

// interruptableSleep will sleep for the specified duration, or until the context is
// cancelled, whichever comes first.  Returns true if the sleep completes, false if
// the context is canceled.
func interruptableSleep(ctx context.Context, d time.Duration) bool {
	timer := clock.NewTimer(ctx, d)
	select {
	case <-ctx.Done():
		timer.Stop()
		return false
	case <-timer.C:
		return true
	}
}

// consumeAndClose will read all the data from the provided io.ReadCloser, then close
// it.  Intended to safely drain HTTP connections.
func consumeAndClose(r io.ReadCloser) {
	_, _ = io.Copy(ioutil.Discard, r)
	_ = r.Close()
}
