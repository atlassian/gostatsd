package cloudproviders

import (
	"fmt"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/cloudproviders/aws"
	"github.com/sirupsen/logrus"
)

// All registered cloud providers.
var providers = map[string]gostatsd.CloudProviderFactory{
	aws.ProviderName: aws.NewProviderFromOptions,
}

// Init creates an instance of the named cloud provider.
func Init(name string, options gostatsd.Options) (gostatsd.CloudProvider, error) {
	factory, found := providers[name]
	if !found {
		return nil, fmt.Errorf("unknown cloud provider %q", name)
	}
	// It's ok to overwrite because options is not a pointer
	options.Logger = options.Logger.WithField("cloud_provider", name)
	provider, err := factory(options)
	if err != nil {
		return nil, fmt.Errorf("could not init cloud provider %q: %v", name, err)
	}
	logrus.Infof("Initialised cloud provider %q", name)

	return provider, nil
}
