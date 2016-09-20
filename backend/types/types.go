package types

import (
	"context"

	"github.com/atlassian/gostatsd/types"

	"github.com/spf13/viper"
)

// Factory is a function that returns a Backend.
type Factory func(*viper.Viper) (Backend, error)

// SendCallback is called by Backend.SendMetricsAsync() to notify about the result of operation.
// A list of errors is passed to the callback. It may be empty or contain nil values. Every non-nil value is an error
// that happened while sending metrics.
type SendCallback func([]error)

// Backend represents a backend.
type Backend interface {
	// BackendName returns the name of the backend.
	BackendName() string
	// SampleConfig returns the sample config for the backend.
	SampleConfig() string
	// SendMetricsAsync flushes the metrics to the backend, preparing payload synchronously but doing the send asynchronously.
	// Must not read/write MetricMap asynchronously.
	SendMetricsAsync(context.Context, *types.MetricMap, SendCallback)
	// SendEvent sends event to the backend.
	SendEvent(context.Context, *types.Event) error
}
