package backends

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/backends/cloudwatch"
	"github.com/atlassian/gostatsd/pkg/backends/datadog"
	"github.com/atlassian/gostatsd/pkg/backends/graphite"
	"github.com/atlassian/gostatsd/pkg/backends/influxdb"
	"github.com/atlassian/gostatsd/pkg/backends/newrelic"
	"github.com/atlassian/gostatsd/pkg/backends/null"
	"github.com/atlassian/gostatsd/pkg/backends/promremotewriter"
	"github.com/atlassian/gostatsd/pkg/backends/statsdaemon"
	"github.com/atlassian/gostatsd/pkg/backends/stdout"
	"github.com/atlassian/gostatsd/pkg/transport"
)

// All known backends.
var backends = map[string]gostatsd.BackendFactory{
	cloudwatch.BackendName:       cloudwatch.NewClientFromViper,
	datadog.BackendName:          datadog.NewClientFromViper,
	graphite.BackendName:         graphite.NewClientFromViper,
	influxdb.BackendName:         influxdb.NewClientFromViper,
	newrelic.BackendName:         newrelic.NewClientFromViper,
	null.BackendName:             null.NewClientFromViper,
	promremotewriter.BackendName: promremotewriter.NewClientFromViper,
	statsdaemon.BackendName:      statsdaemon.NewClientFromViper,
	stdout.BackendName:           stdout.NewClientFromViper,
}

// GetBackend creates an instance of the named backend, or nil if
// the name is not known. The error return is only used if the named backend
// was known but failed to initialize.
func GetBackend(name string, v *viper.Viper, logger logrus.FieldLogger, pool *transport.TransportPool) (gostatsd.Backend, error) {
	f, found := backends[name]
	if !found {
		return nil, nil
	}
	return f(v, logger, pool)
}

// InitBackend creates an instance of the named backend.
func InitBackend(name string, v *viper.Viper, logger logrus.FieldLogger, pool *transport.TransportPool) (gostatsd.Backend, error) {
	if name == "" {
		logger.Info("No backend specified")
		return nil, nil
	}

	logger = logger.WithField("backend", name)

	backend, err := GetBackend(name, v, logger, pool)
	if err != nil {
		return nil, fmt.Errorf("could not init backend %q: %v", name, err)
	}
	if backend == nil {
		return nil, fmt.Errorf("unknown backend %q", name)
	}
	logger.Info("Initialised backend")

	return backend, nil
}
