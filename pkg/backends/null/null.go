package null

import (
	"context"

	"github.com/atlassian/gostatsd"

	"github.com/spf13/viper"
)

// BackendName is the name of this backend.
const BackendName = "null"

// client represents a discarding backend.
type client struct{}

// NewClientFromViper constructs a GraphiteClient object by connecting to an address.
func NewClientFromViper(v *viper.Viper) (gostatsd.Backend, error) {
	return NewClient()
}

// NewClient constructs a client object.
func NewClient() (gostatsd.Backend, error) {
	return client{}, nil
}

// SampleConfig returns the sample config for the null backend.
func (client client) SampleConfig() string {
	return ""
}

// SendMetricsAsync discards the metrics in a MetricsMap.
func (client client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	cb(nil)
}

// SendEvent discards events.
func (client client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	return nil
}

// BackendName returns the name of the backend.
func (client client) Name() string {
	return BackendName
}
