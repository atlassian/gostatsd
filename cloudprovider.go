package gostatsd

import (
	"context"

	"github.com/spf13/viper"
)

// CloudProviderFactory is a function that returns a CloudProvider.
type CloudProviderFactory func(*viper.Viper) (CloudProvider, error)

// Instance represents a cloud instance.
type Instance struct {
	ID     string
	Region string
	Tags   Tags
}

// CloudProvider represents a cloud provider.
type CloudProvider interface {
	// ProviderName returns the name of the cloud provider.
	Name() string
	// SampleConfig returns the sample config for the cloud provider.
	SampleConfig() string
	// Instance returns the instance details from the cloud provider.
	Instance(context.Context, IP) (*Instance, error)
	// SelfIP returns host's IPv4 address.
	SelfIP() (IP, error)
}
