package statsd

import (
	"context"
	"time"

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

// ProcessFunc is a function that gets executed by Aggregator with its state passed into the function.
type ProcessFunc func(*gostatsd.MetricMap)

// Aggregator is an object that aggregates statsd metrics.
// The function NewAggregator should be used to create the objects.
//
// Incoming metrics should be passed via Receive function.
type Aggregator interface {
	Receive(*gostatsd.Metric, time.Time)
	Flush(interval time.Duration)
	Process(ProcessFunc)
	Reset()
}
