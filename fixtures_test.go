package gostatsd

import (
	"context"
	"time"
)

// testContext returns a context that will timeout and fail the test if not canceled.  Used to
// enforce a timeout on tests.
func testContext() (context.Context, func()) {
	ctxTest, completeTest := context.WithTimeout(context.Background(), 1100*time.Millisecond)
	go func() {
		after := time.NewTimer(1 * time.Second)
		select {
		case <-ctxTest.Done():
			after.Stop()
		case <-after.C:
			panic("test timed out")
		}
	}()
	return ctxTest, completeTest
}
