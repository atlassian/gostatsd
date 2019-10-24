package gostatsd

import (
	"context"
)

// SendCallback is called by Backend.SendMetricsAsync() to notify about the result of operation.
// A list of errors is passed to the callback. It may be empty or contain nil values. Every non-nil value is an error
// that happened while sending metrics.
type SendCallback func([]error)

// Backend represents a backend.
// If Backend implements the Runner interface, it's started in a new goroutine at creation.
type Backend interface {
	// Name returns the name of the backend.
	Name() string
	// SendMetricsAsync flushes the metrics to the backend, preparing payload synchronously but doing the send asynchronously.
	// Must not read/write MetricMap asynchronously.
	SendMetricsAsync(context.Context, *MetricMap, SendCallback)
	// SendEvent sends event to the backend.
	SendEvent(context.Context, *Event) error
}
