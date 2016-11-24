package null

import (
	"context"

	"github.com/atlassian/gostatsd"
	backendTypes "github.com/atlassian/gostatsd/backend/types"

	"github.com/spf13/viper"
)

// BackendName is the name of this backend.
const BackendName = "null"

// client represents a discarding backend.
type client struct{}

// NewClientFromViper constructs a GraphiteClient object by connecting to an address.
func NewClientFromViper(v *viper.Viper) (backendTypes.Backend, error) {
	return NewClient()
}

// NewClient constructs a client object.
func NewClient() (backendTypes.Backend, error) {
	return client{}, nil
}

// SampleConfig returns the sample config for the null backend.
func (client client) SampleConfig() string {
	return ""
}

// SendMetricsAsync discards the metrics in a MetricsMap.
func (client client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb backendTypes.SendCallback) {
	cb(nil)
}

// SendEvent discards events.
func (client client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	return nil
}

// BackendName returns the name of the backend.
func (client client) BackendName() string {
	return BackendName
}
