package null

import (
	"github.com/atlassian/gostatsd/backend"
	"github.com/atlassian/gostatsd/types"

	"github.com/spf13/viper"
)

const backendName = "null"

func init() {
	backend.RegisterBackend(backendName, func(v *viper.Viper) (backend.MetricSender, error) {
		return NewClient()
	})
}

// Client is an object that is used to send messages to stdout.
type Client struct{}

// NewClient constructs a StdoutClient object.
func NewClient() (backend.MetricSender, error) {
	return &Client{}, nil
}

// SampleConfig returns the sample config for the stdout backend.
func (client *Client) SampleConfig() string {
	return ""
}

// SendMetrics discards the metrics in a MetricsMap.
func (client *Client) SendMetrics(metrics types.MetricMap) error {
	return nil
}

// BackendName returns the name of the backend.
func (client *Client) BackendName() string {
	return backendName
}
