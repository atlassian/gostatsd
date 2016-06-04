package types

import (
	"github.com/atlassian/gostatsd/types"

	"github.com/spf13/viper"
)

// Factory is a function that returns a cloud provider Interface.
type Factory func(*viper.Viper) (Interface, error)

// Instance represents a cloud instance.
type Instance struct {
	ID     string
	Region string
	Tags   types.Tags
}

// Interface represents a cloud provider.
type Interface interface {
	// ProviderName returns the name of the cloud provider.
	ProviderName() string
	// SampleConfig returns the sample config for the cloud provider.
	SampleConfig() string
	// Instance returns the instance details from the cloud provider.
	Instance(types.IP) (*Instance, error)
}
