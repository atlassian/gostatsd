package backend

import (
	"fmt"
	"sync"

	"github.com/jtblin/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
)

// All registered auth backends.
var backendsMutex sync.Mutex
var backends = make(map[string]Factory)

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

// The MetricSenderFunc type is an adapter to allow the use of ordinary functions as metric senders.
type MetricSenderFunc func(types.MetricMap) error

// SendMetrics calls f(m).
func (f MetricSenderFunc) SendMetrics(m types.MetricMap) error {
	return f(m)
}

// BackendName returns the name of the backend.
func (f MetricSenderFunc) BackendName() string {
	return "MetricSenderFunc"
}

// RegisterBackend registers an authentication backend.
func RegisterBackend(name string, backend Factory) {
	backendsMutex.Lock()
	defer backendsMutex.Unlock()
	if _, found := backends[name]; found {
		log.Fatalf("Backend %q was registered twice", name)
	}
	log.Infof("Registered backend %q", name)
	backends[name] = backend
}

// GetBackend creates an instance of the named backend, or nil if
// the name is not known.  The error return is only used if the named backend
// was known but failed to initialize.
func GetBackend(name string, v *viper.Viper) (MetricSender, error) {
	backendsMutex.Lock()
	defer backendsMutex.Unlock()
	f, found := backends[name]
	if !found {
		return nil, nil
	}
	return f(v)
}

// InitBackend creates an instance of the named backend.
func InitBackend(name string, v *viper.Viper) (MetricSender, error) {
	if name == "" {
		log.Info("No backend specified.")
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
