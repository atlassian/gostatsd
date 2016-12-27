package statsd

import (
	"context"
	"sync"
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

// DispatcherProcessFunc is a function that gets executed by Dispatcher for each Aggregator, passing it into the function.
type DispatcherProcessFunc func(uint16, Aggregator)

// Dispatcher is responsible for managing Aggregators' lifecycle and dispatching metrics among them.
type Dispatcher interface {
	// DispatchMetric dispatches metric to a corresponding Aggregator.
	DispatchMetric(context.Context, *gostatsd.Metric) error
	// Process concurrently executes provided function in goroutines that own Aggregators.
	// DispatcherProcessFunc function may be executed zero or up to numWorkers times. It is executed
	// less than numWorkers times if the context signals "done".
	Process(context.Context, DispatcherProcessFunc) *sync.WaitGroup
}
