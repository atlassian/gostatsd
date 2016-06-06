package statsd

import (
	backendTypes "github.com/atlassian/gostatsd/backend/types"
	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

// dispatchingHandler dispatches events to all configured backends and forwards metrics to a Dispatcher.
type dispatchingHandler struct {
	dispatcher Dispatcher
	backends   []backendTypes.Backend
}

// NewDispatchingHandler initialises a new dispatching handler.
func NewDispatchingHandler(dispatcher Dispatcher, backends []backendTypes.Backend) Handler {
	return &dispatchingHandler{
		dispatcher: dispatcher,
		backends:   backends,
	}
}

func (dh *dispatchingHandler) DispatchMetric(ctx context.Context, m *types.Metric) error {
	if m.Hostname == "" {
		m.Hostname = string(m.SourceIP)
	}
	return dh.dispatcher.DispatchMetric(ctx, m)
}

func (dh *dispatchingHandler) DispatchEvent(ctx context.Context, e *types.Event) error {
	if e.Hostname == "" {
		e.Hostname = string(e.SourceIP)
	}
	for _, backend := range dh.backends {
		go func(b backendTypes.Backend) {
			if err := b.SendEvent(ctx, e); err != nil {
				log.Errorf("Sending event to backend failed: %v", err)
			}
		}(backend)
	}
	return nil
}
