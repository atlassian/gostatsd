package cloudprovider

import (
	"fmt"

	"github.com/atlassian/gostatsd/cloudprovider/providers/aws"
	cloudTypes "github.com/atlassian/gostatsd/cloudprovider/types"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
)

// All registered cloud providers.
var providers = map[string]cloudTypes.Factory{
	aws.ProviderName: aws.NewProviderFromViper,
}

// GetCloudProvider creates an instance of the named provider, or nil if
// the name is not known.  The error return is only used if the named provider
// was known but failed to initialize.
func GetCloudProvider(name string, v *viper.Viper) (cloudTypes.Interface, error) {
	f, found := providers[name]
	if !found {
		return nil, nil
	}
	return f(v)
}

// InitCloudProvider creates an instance of the named cloud provider.
func InitCloudProvider(name string, v *viper.Viper) (cloudTypes.Interface, error) {
	if name == "" {
		log.Info("No cloud provider specified")
		return nil, nil
	}

	provider, err := GetCloudProvider(name, v)
	if err != nil {
		return nil, fmt.Errorf("could not init cloud provider %q: %v", name, err)
	}
	if provider == nil {
		return nil, fmt.Errorf("unknown cloud provider %q", name)
	}
	log.Infof("Initialised cloud provider %q", name)

	return provider, nil
}
