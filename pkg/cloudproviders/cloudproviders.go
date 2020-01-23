package cloudproviders

import (
	"fmt"

	"github.com/atlassian/gostatsd/pkg/cloudproviders/k8s"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/cloudproviders/aws"

	"github.com/sirupsen/logrus"
)

// All registered cloud providers.
var providers = map[string]gostatsd.CloudProviderFactory{
	aws.ProviderName: aws.NewProviderFromOptions,
	k8s.ProviderName: k8s.NewProviderFromOptions,
}

// Get creates an instance of the named provider, or nil if
// the name is not known.  The error return is only used if the named provider
// was known but failed to initialize.
func Get(name string, options gostatsd.CloudProviderOptions) (gostatsd.CloudProvider, error) {
	f, found := providers[name]
	if !found {
		return nil, nil
	}
	return f(options)
}

// Init creates an instance of the named cloud provider.
func Init(name string, options gostatsd.CloudProviderOptions) (gostatsd.CloudProvider, error) {
	if name == "" {
		logrus.Info("No cloud provider specified")
		return nil, nil
	}

	provider, err := Get(name, options)
	if err != nil {
		return nil, fmt.Errorf("could not init cloud provider %q: %v", name, err)
	}
	if provider == nil {
		return nil, fmt.Errorf("unknown cloud provider %q", name)
	}
	logrus.Infof("Initialised cloud provider %q", name)

	return provider, nil
}
