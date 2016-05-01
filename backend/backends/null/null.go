package null

import (
	backendTypes "github.com/atlassian/gostatsd/backend/types"
	"github.com/atlassian/gostatsd/types"

	"github.com/spf13/viper"
)

// BackendName is the name of this backend.
const BackendName = "null"

// client represents a discarding backend.
type client struct{}

// NewClientFromViper constructs a GraphiteClient object by connecting to an address.
func NewClientFromViper(v *viper.Viper) (backendTypes.MetricSender, error) {
	return NewClient()
}

// NewClient constructs a client object.
func NewClient() (backendTypes.MetricSender, error) {
	return client{}, nil
}

// SampleConfig returns the sample config for the null backend.
func (client client) SampleConfig() string {
	return ""
}

// SendMetrics discards the metrics in a MetricsMap.
func (client client) SendMetrics(metrics types.MetricMap) error {
	return nil
}

// BackendName returns the name of the backend.
func (client client) BackendName() string {
	return BackendName
}
