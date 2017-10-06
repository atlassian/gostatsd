package statser

import (
	"sync"
	"time"
)

type flushNotifier struct {
	lock         sync.RWMutex
	flushTargets []chan<- time.Duration
}

// RegisterFlush registers a channel which will receive a notification after every flush. If
// the channel blocks, the notification will be silently dropped.  Thread-safe.
func (fn *flushNotifier) RegisterFlush() (<-chan time.Duration, func()) {
	f := make(chan time.Duration)
	fn.lock.Lock()
	defer fn.lock.Unlock()
	fn.flushTargets = append(fn.flushTargets, f)
	return f, func() {
		fn.lock.Lock()
		defer fn.lock.Unlock()

		targets := fn.flushTargets[:0]
		for _, target := range fn.flushTargets {
			if target != f {
				targets = append(targets, target)
			}
		}
		fn.flushTargets = targets
		close(f)
	}
}

// NotifyFlush will notify any registered channels that a flush has completed.
// Non-blocking, thread-safe.
func (fn *flushNotifier) NotifyFlush(d time.Duration) {
	fn.lock.RLock()
	defer fn.lock.RUnlock()
	for _, hook := range fn.flushTargets {
		select {
		case hook <- d:
			// great success
		default:
			// we tried
		}
	}
}
