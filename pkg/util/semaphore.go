package util

import (
	"context"
)

// Semaphore provides a semaphore interface with Acquire operations taking a context.
type Semaphore interface {
	// Acquire will attempt to acquire a lock on the semaphore.  Returns true if successful, and false is the
	// context is cancelled.
	Acquire(ctx context.Context) bool
	Release()
}

// NewSemaphore returns a new Semaphore with a capacity of the provided count.  If count is zero, the capacity
// is unlimited.
func NewSemaphore(count int) Semaphore {
	if count == 0 {
		return &nullSemaphore{}
	}
	ch := make(chan struct{}, count)
	for i := 0; i < count; i++ {
		ch <- struct{}{}
	}
	return &chanSemaphore{
		sem: ch,
	}
}

type chanSemaphore struct {
	sem chan struct{}
}

func (c *chanSemaphore) Acquire(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-c.sem:
		return true
	}

}

func (c *chanSemaphore) Release() {
	c.sem <- struct{}{}
}

type nullSemaphore struct{}

func (ns *nullSemaphore) Acquire(ctx context.Context) bool { return true }
func (ns *nullSemaphore) Release()                         {}
