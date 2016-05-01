package types

import (
	"github.com/atlassian/gostatsd/types"

	"github.com/spf13/viper"
)

// Factory is a function that returns a MetricSender.
type Factory func(v *viper.Viper) (MetricSender, error)

// MetricSender represents a backend.
type MetricSender interface {
	// BackendName returns the name of the backend.
	BackendName() string
	// SampleConfig returns the sample config for the backend.
	SampleConfig() string
	// SendMetrics flushes the metrics to the backend.
	SendMetrics(metrics types.MetricMap) error
}
