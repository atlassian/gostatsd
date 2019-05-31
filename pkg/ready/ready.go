package ready

import (
	"context"
	"sync"
)

type keyType int
const wgKey = keyType(0)

func fromContext(ctx context.Context) (*sync.WaitGroup, bool) {
	wg, ok := ctx.Value(wgKey).(*sync.WaitGroup)
	return wg, ok
}

// WithWaitGroup will attach a *sync.WaitGroup to a context.Context.
func WithWaitGroup(ctx context.Context, wg *sync.WaitGroup) context.Context {
	return context.WithValue(ctx, wgKey, wg)
}

// SignalReady will call wg.Done if there is a *sync.WaitGroup attached to this context.
func SignalReady(ctx context.Context) {
	wg, ok := fromContext(ctx)
	if ok {
		wg.Done()
	}
}

// Add will call wg.Add if there is a *sync.WaitGroup attached to this context.
func Add(ctx context.Context, n int) {
	wg, ok := fromContext(ctx)
	if ok {
		wg.Add(n)
	}
}
