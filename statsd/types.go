package statsd

import (
	"context"

	"github.com/atlassian/gostatsd"
)

// Handler interface can be used to handle metrics and events.
type Handler interface {
	// DispatchMetric dispatches metric to the next step in a pipeline.
	DispatchMetric(context.Context, *gostatsd.Metric) error
	// DispatchEvent dispatches event to the next step in a pipeline.
	DispatchEvent(context.Context, *gostatsd.Event) error
	// WaitForEvents waits for all event-dispatching goroutines to finish.
	WaitForEvents()
}
