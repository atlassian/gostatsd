package statsd

import (
	"context"
	"sync"

	backendTypes "github.com/atlassian/gostatsd/backend/types"
	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
)

// dispatchingHandler dispatches events to all configured backends and forwards metrics to a Dispatcher.
type dispatchingHandler struct {
	wg               sync.WaitGroup
	dispatcher       Dispatcher
	backends         []backendTypes.Backend
	tags             types.Tags // Tags to add to all metrics and events
	concurrentEvents chan struct{}
}

// NewDispatchingHandler initialises a new dispatching handler.
func NewDispatchingHandler(dispatcher Dispatcher, backends []backendTypes.Backend, tags types.Tags, maxConcurrentEvents uint) Handler {
	return &dispatchingHandler{
		dispatcher:       dispatcher,
		backends:         backends,
		tags:             tags,
		concurrentEvents: make(chan struct{}, maxConcurrentEvents),
	}
}

func (dh *dispatchingHandler) DispatchMetric(ctx context.Context, m *types.Metric) error {
	if m.Hostname == "" {
		m.Hostname = string(m.SourceIP)
	}
	m.Tags = append(m.Tags, dh.tags...)
	return dh.dispatcher.DispatchMetric(ctx, m)
}

func (dh *dispatchingHandler) DispatchEvent(ctx context.Context, e *types.Event) error {
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
func (dh *dispatchingHandler) WaitForEvents() {
	dh.wg.Wait()
}

func (dh *dispatchingHandler) dispatchEvent(ctx context.Context, backend backendTypes.Backend, e *types.Event) {
	defer dh.wg.Done()
	defer func() {
		<-dh.concurrentEvents
	}()
	if err := backend.SendEvent(ctx, e); err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		log.Errorf("Sending event to backend failed: %v", err)
	}
}
