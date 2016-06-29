package statsd

import (
	"sync"

	backendTypes "github.com/atlassian/gostatsd/backend/types"
	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

// dispatchingHandler dispatches events to all configured backends and forwards metrics to a Dispatcher.
type dispatchingHandler struct {
	wg         sync.WaitGroup
	dispatcher Dispatcher
	backends   []backendTypes.Backend
	tags       types.Tags // Tags to add to all metrics and events
}

// NewDispatchingHandler initialises a new dispatching handler.
func NewDispatchingHandler(dispatcher Dispatcher, backends []backendTypes.Backend, tags types.Tags) *dispatchingHandler {
	return &dispatchingHandler{
		dispatcher: dispatcher,
		backends:   backends,
		tags:       tags,
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
	dh.wg.Add(len(dh.backends))
	for _, backend := range dh.backends {
		go func(b backendTypes.Backend) {
			defer dh.wg.Done()
			if err := b.SendEvent(ctx, e); err != nil && err != context.Canceled && err != context.DeadlineExceeded {
				log.Errorf("Sending event to backend failed: %v", err)
			}
		}(backend)
	}
	return nil
}

// WaitForEvents waits for all event-dispatching goroutines to finish.
func (dh *dispatchingHandler) WaitForEvents() {
	dh.wg.Wait()
}
