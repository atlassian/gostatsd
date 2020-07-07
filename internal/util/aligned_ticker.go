package util

import (
	"context"
	"time"

	"github.com/tilinna/clock"
)

// AlignedTicker is a wrapper around time.Ticker will align the time on a specific interval.  The first tick will
// always occur within interval, the caller should explicitly discard this on creation, or not.
// Instead of firing at:
// [T+1*interval, T+2*interval, T+3*interval, ...]
//
// It will fire at:
// r = roundup(T, interval)+offset
// [r+1*interval, r+interval*2, r+3*interval, ...]
//
// The time.Time sent to the channel is guaranteed to be r+offset+n*interval, rather than the actual time of firing.
type AlignedTicker struct {
	C          <-chan time.Time
	chInternal chan time.Time
	chStop     chan struct{}
	interval   time.Duration
	offset     time.Duration
}

func NewAlignedTicker(interval, offset time.Duration) *AlignedTicker {
	return NewAlignedTickerWithContext(context.Background(), interval, offset)
}

func NewAlignedTickerWithContext(ctx context.Context, interval, offset time.Duration) *AlignedTicker {
	ch := make(chan time.Time, 1)
	at := &AlignedTicker{
		C:          ch,
		chInternal: ch,
		chStop:     make(chan struct{}),
		interval:   interval,
		offset:     offset,
	}
	go at.start(ctx)
	return at
}

func roundup(t time.Time, i time.Duration) time.Time {
	return t.Truncate(i).Add(i)
}

func (at *AlignedTicker) start(ctx context.Context) {
	clck := clock.FromContext(ctx)
	now := clck.Now()
	initialWait := roundup(now.Add(-at.offset), at.interval).Add(at.offset).Sub(now)
	tmr := clck.NewTimer(initialWait)
	var tckr *clock.Ticker

	// Phase 1: wait for the next interval
	select {
	case now := <-tmr.C:
		// Start the repeating timer as soon as possible
		tckr = clck.NewTicker(at.interval)
		defer tckr.Stop()
		if !at.sendTick(now) {
			tmr.Stop()
			return
		}
	case <-at.chStop:
		tmr.Stop()
		return
	}

	// Phase 2: process the ticker
	for {
		select {
		case now := <-tckr.C:
			if !at.sendTick(now) {
				return
			}
		case <-at.chStop:
			tckr.Stop()
			return
		}
	}
}

func (at *AlignedTicker) sendTick(t time.Time) bool {
	rounded := t.Add(-at.offset).Truncate(at.interval).Add(at.offset)
	select {
	case at.chInternal <- rounded:
		return true
	case <-at.chStop:
		return false
	default:
		return true
	}
}

func (at *AlignedTicker) Stop() {
	close(at.chStop)
}
