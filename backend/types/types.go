package types

import (
	"github.com/atlassian/gostatsd/types"

	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

// Factory is a function that returns a Backend.
type Factory func(*viper.Viper) (Backend, error)

// Backend represents a backend.
type Backend interface {
	// BackendName returns the name of the backend.
	BackendName() string
	// SampleConfig returns the sample config for the backend.
	SampleConfig() string
	// SendMetrics flushes the metrics to the backend.
	SendMetrics(context.Context, *types.MetricMap) error
	// SendEvent sends event to the backend.
	SendEvent(context.Context, *types.Event) error
}
