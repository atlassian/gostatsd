package backend

import (
	"fmt"

	"github.com/atlassian/gostatsd/backend/backends/datadog"
	"github.com/atlassian/gostatsd/backend/backends/graphite"
	"github.com/atlassian/gostatsd/backend/backends/null"
	"github.com/atlassian/gostatsd/backend/backends/statsdaemon"
	"github.com/atlassian/gostatsd/backend/backends/stdout"
	backendTypes "github.com/atlassian/gostatsd/backend/types"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
)

// All known backends.
var backends = map[string]backendTypes.Factory{
	datadog.BackendName:     datadog.NewClientFromViper,
	graphite.BackendName:    graphite.NewClientFromViper,
	null.BackendName:        null.NewClientFromViper,
	statsdaemon.BackendName: statsdaemon.NewClientFromViper,
	stdout.BackendName:      stdout.NewClientFromViper,
}

// GetBackend creates an instance of the named backend, or nil if
// the name is not known. The error return is only used if the named backend
// was known but failed to initialize.
func GetBackend(name string, v *viper.Viper) (backendTypes.Backend, error) {
	f, found := backends[name]
	if !found {
		return nil, nil
	}
	return f(v)
}

// InitBackend creates an instance of the named backend.
func InitBackend(name string, v *viper.Viper) (backendTypes.Backend, error) {
	if name == "" {
		log.Info("No backend specified")
		return nil, nil
	}

	backend, err := GetBackend(name, v)
	if err != nil {
		return nil, fmt.Errorf("could not init backend %q: %v", name, err)
	}
	if backend == nil {
		return nil, fmt.Errorf("unknown backend %q", name)
	}
	log.Infof("Initialised backend %q", name)

	return backend, nil
}
