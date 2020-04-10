package gostatsd

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// CloudProviderFactory is a function that returns a CloudProvider.
type CloudProviderFactory func(v *viper.Viper, logger logrus.FieldLogger, version string) (CloudProvider, error)

// Instance represents a cloud instance.
type Instance struct {
	ID   string
	Tags Tags
}

// CloudProvider represents a cloud provider.
// If CloudProvider implements the Runner interface, it's started in a new goroutine at creation.
type CloudProvider interface {
	// Name returns the name of the cloud provider.
	Name() string
	// Instance returns instances details from the cloud provider.
	// ip -> nil pointer if instance was not found.
	// map is returned even in case of errors because it may contain partial data.
	Instance(context.Context, ...IP) (map[IP]*Instance, error)
	// MaxInstancesBatch returns maximum number of instances that could be requested via the Instance method.
	MaxInstancesBatch() int
	// SelfIP returns host's IPv4 address.
	SelfIP() (IP, error)
	// EstimatedTags returns a guess of how many tags are likely to be added by the CloudProvider
	EstimatedTags() int
}

type InstanceInfo struct {
	IP IP
	// Instance may be nil if the lookup resulted in an error or instance was not found.
	Instance *Instance
}

type CachedInstances interface {
	// Peek fetches instance information from the cache.
	// The cache is also a negative cache - may be a cache hit but the returned instance is nil.
	Peek(IP) (*Instance, bool /*is a cache hit*/)
	// IpSink returns a channel that can be used to supply IP addresses for which information needs
	// to be fetched and cached.
	IpSink() chan<- IP
	// InfoSource returns a channel that can be used to receive information about IPs.
	InfoSource() <-chan InstanceInfo
	// EstimatedTags returns a guess for how many tags to pre-allocate
	EstimatedTags() int
}

// CacheOptions holds cache behaviour configuration.
type CacheOptions struct {
	CacheRefreshPeriod        time.Duration
	CacheEvictAfterIdlePeriod time.Duration
	CacheTTL                  time.Duration
	CacheNegativeTTL          time.Duration
}
