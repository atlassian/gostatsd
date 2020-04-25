package cachedinstances

import (
	"errors"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/cachedinstances/k8s"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	// All registered native CachedInstances implementations.
	providers = map[string]gostatsd.CachedInstancesFactory{
		k8s.ProviderName: k8s.NewProviderFromViper,
	}

	ErrUnknownProvider = errors.New("unknown cloud provider")
)

// Get creates an instance of the named provider.
func Get(logger logrus.FieldLogger, name string, v *viper.Viper, version string) (gostatsd.CachedInstances, error) {
	f, found := providers[name]
	if !found {
		return nil, ErrUnknownProvider
	}
	return f(v, logger.WithField("cloud_provider", name), version)
}
