package statsd

import (
	"context"
	"sync"

	"github.com/atlassian/gostatsd"

	log "github.com/sirupsen/logrus"
)

// DispatchingHandler dispatches events to all configured backends and forwards metrics to a Dispatcher.
type DispatchingHandler struct {
	wg               sync.WaitGroup
	dispatcher       Dispatcher
	backends         []gostatsd.Backend
	tags             gostatsd.Tags // Tags to add to all metrics and events
	concurrentEvents chan struct{}
}

// NewDispatchingHandler initialises a new dispatching handler.
func NewDispatchingHandler(dispatcher Dispatcher, backends []gostatsd.Backend, tags gostatsd.Tags, maxConcurrentEvents uint) *DispatchingHandler {
	return &DispatchingHandler{
		dispatcher:       dispatcher,
		backends:         backends,
		tags:             tags,
		concurrentEvents: make(chan struct{}, maxConcurrentEvents),
	}
}

func (dh *DispatchingHandler) DispatchMetric(ctx context.Context, m *gostatsd.Metric) error {
	if m.Hostname == "" {
		m.Hostname = string(m.SourceIP)
	}
	m.Tags = append(m.Tags, dh.tags...)
	return dh.dispatcher.DispatchMetric(ctx, m)
}

func (dh *DispatchingHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) error {
	if e.Hostname == "" {
		e.Hostname = string(e.SourceIP)
	}
	e.Tags = append(e.Tags, dh.tags...)
	eventsDispatched := 0
	dh.wg.Add(len(dh.backends))
	for _, backend := range dh.backends {
		select {
		case <-ctx.Done():
			// Not all backends got the event, should decrement the wg counter
			dh.wg.Add(eventsDispatched - len(dh.backends))
			return ctx.Err()
		case dh.concurrentEvents <- struct{}{}:
			go dh.dispatchEvent(ctx, backend, e)
			eventsDispatched++
		}
	}
	return nil
}

// WaitForEvents waits for all event-dispatching goroutines to finish.
func (dh *DispatchingHandler) WaitForEvents() {
	dh.wg.Wait()
}

func (dh *DispatchingHandler) dispatchEvent(ctx context.Context, backend gostatsd.Backend, e *gostatsd.Event) {
	defer dh.wg.Done()
	defer func() {
		<-dh.concurrentEvents
	}()
	if err := backend.SendEvent(ctx, e); err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		log.Errorf("Sending event to backend failed: %v", err)
	}
}
