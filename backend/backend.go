package backend

import (
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/jtblin/gostatsd/types"
)

// All registered auth backends.
var backendsMutex sync.Mutex
var backends = make(map[string]Factory)

// Factory is a function that returns a MetricSender.
type Factory func() (MetricSender, error)

// MetricSender represents a backend
type MetricSender interface {
	// Name returns the name of the backend
	Name() string
	// ProcessFlush flushes the metrics to the backend
	SendMetrics(metrics types.MetricMap) error
}

// The MetricSenderFunc type is an adapter to allow the use of ordinary functions as metric senders
type MetricSenderFunc func(types.MetricMap) error

// SendMetrics calls f(m)
func (f MetricSenderFunc) SendMetrics(m types.MetricMap) error {
	return f(m)
}

// Name returns the name
func (f MetricSenderFunc) Name() string {
	return "MetricSenderFunc"
}

// RegisterBackend registers an authentication backend
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
// the name is not known.  The error return is only used if the named provider
// was known but failed to initialize. The config parameter specifies the
// io.Reader handler of the configuration file for the authenticator backend, or nil
// for no configuation.
func GetBackend(name string) (MetricSender, error) {
	backendsMutex.Lock()
	defer backendsMutex.Unlock()
	f, found := backends[name]
	if !found {
		return nil, nil
	}
	return f()
}

// InitBackend creates an instance of the named backend.
func InitBackend(name string) (MetricSender, error) {
	if name == "" {
		log.Info("No backend specified.")
		return nil, nil
	}

	backend, err := GetBackend(name)
	if err != nil {
		return nil, fmt.Errorf("could not init backend %q: %v", name, err)
	}
	if backend == nil {
		return nil, fmt.Errorf("unknown backend %q", name)
	}
	log.Infof("Backend %q initialised.", name)

	return backend, nil
}
