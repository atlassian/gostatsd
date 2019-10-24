package util

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSemaphoreUnlimited(t *testing.T) {
	t.Parallel()
	s := NewSemaphore(0)
	for i := 0; i < 10; i++ {
		s.Acquire(context.Background())
	}
	for i := 0; i < 10; i++ {
		s.Release()
	}
}

func TestSemaphoreNormal(t *testing.T) {
	t.Parallel()
	s := NewSemaphore(5)
	var c uint64
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			s.Acquire(context.Background())
			ctr := atomic.AddUint64(&c, 1)
			require.LessOrEqual(t, ctr, uint64(5))
			atomic.AddUint64(&c, ^uint64(0))
			s.Release()
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestSemaphoreCancelled(t *testing.T) {
	t.Parallel()
	s := NewSemaphore(1)
	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	require.True(t, s.Acquire(context.Background()))
	require.False(t, s.Acquire(cancelledContext))
	s.Release()
}
