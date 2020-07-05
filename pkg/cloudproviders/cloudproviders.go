package cloudproviders

import (
	"errors"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/cloudproviders/aws"
)

var (
	// All registered cloud providers.
	providers = map[string]gostatsd.CloudProviderFactory{
		aws.ProviderName: aws.NewProviderFromViper,
	}

	ErrUnknownProvider = errors.New("unknown cloud provider")
)

// Get creates an instance of the named provider.
func Get(logger logrus.FieldLogger, name string, v *viper.Viper, version string) (gostatsd.CloudProvider, error) {
	f, found := providers[name]
	if !found {
		return nil, ErrUnknownProvider
	}
	return f(v, logger.WithField("cloud_provider", name), version)
}
