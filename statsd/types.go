package statsd

import (
	"context"

	"github.com/atlassian/gostatsd/types"
)

// Handler interface can be used to handle metrics and events.
type Handler interface {
	// DispatchMetric dispatches metric to the next step in a pipeline.
	DispatchMetric(context.Context, *types.Metric) error
	// DispatchEvent dispatches event to the next step in a pipeline.
	DispatchEvent(context.Context, *types.Event) error
	// WaitForEvents waits for all event-dispatching goroutines to finish.
	WaitForEvents()
}
