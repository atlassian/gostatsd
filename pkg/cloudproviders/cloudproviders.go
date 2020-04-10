package cloudproviders

import (
	"fmt"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/cloudproviders/aws"
	"github.com/atlassian/gostatsd/pkg/cloudproviders/k8s"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// All registered cloud providers.
var providers = map[string]gostatsd.CloudProviderFactory{
	aws.ProviderName: aws.NewProviderFromViper,
	k8s.ProviderName: k8s.NewProviderFromViper,
}

// Get creates an instance of the named provider.
func Get(logger logrus.FieldLogger, name string, v *viper.Viper, version string) (gostatsd.CloudProvider, error) {
	f, found := providers[name]
	if !found {
		return nil, fmt.Errorf("unknown cloud provider %q", name)
	}
	return f(v, logger.WithField("cloud_provider", name), version)
}
