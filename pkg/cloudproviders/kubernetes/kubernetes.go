package kubernetes

import (
	"context"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	// ProviderName is the name of Kubernetes cloud provider.
	ProviderName             = "kubernetes"
	defaultClientTimeout     = 9 * time.Second
	defaultMaxInstancesBatch = 32
)

// Provider represents an AWS provider.
type Provider struct {
	//describeInstanceCount     uint64 // The cumulative number of times DescribeInstancesPagesWithContext has been called
	//describeInstanceInstances uint64 // The cumulative number of instances which have been fed in to DescribeInstancesPagesWithContext
	//describeInstancePages     uint64 // The cumulative number of pages from DescribeInstancesPagesWithContext
	//describeInstanceErrors    uint64 // The cumulative number of errors seen from DescribeInstancesPagesWithContext
	//describeInstanceFound     uint64 // The cumulative number of instances successfully found via DescribeInstancesPagesWithContext

	logger logrus.FieldLogger
}

func (p *Provider) EstimatedTags() int {
	return 10 + 1 // 10 for EC2 tags, 1 for the region
}

func (p *Provider) RunMetrics(ctx context.Context, statser stats.Statser) {
	flushed, unregister := statser.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushed:
			// These are namespaced not tagged because they're very specific
			//statser.Gauge("cloudprovider.kubernetes.describeinstancecount", float64(atomic.LoadUint64(&p.describeInstanceCount)), nil)
			//statser.Gauge("cloudprovider.kubernetes.describeinstanceinstances", float64(atomic.LoadUint64(&p.describeInstanceInstances)), nil)
			//statser.Gauge("cloudprovider.kubernetes.describeinstancepages", float64(atomic.LoadUint64(&p.describeInstancePages)), nil)
			//statser.Gauge("cloudprovider.kubernetes.describeinstanceerrors", float64(atomic.LoadUint64(&p.describeInstanceErrors)), nil)
			//statser.Gauge("cloudprovider.kubernetes.describeinstancefound", float64(atomic.LoadUint64(&p.describeInstanceFound)), nil)
		}
	}
}

// Instance returns instances details from AWS.
// ip -> nil pointer if instance was not found.
// map is returned even in case of errors because it may contain partial data.
func (p *Provider) Instance(ctx context.Context, IP ...gostatsd.IP) (map[gostatsd.IP]*gostatsd.Instance, error) {

	return instances, nil
}

// MaxInstancesBatch returns maximum number of instances that could be requested via the Instance method.
func (p *Provider) MaxInstancesBatch() int {
	return 64 // this is arbitrary and could be more, but would not make sense.
}

// Name returns the name of the provider.
func (p *Provider) Name() string {
	return ProviderName
}

// SelfIP returns host's IPv4 address.
func (p *Provider) SelfIP() (gostatsd.IP, error) {
}

// NewProviderFromViper returns a new aws provider.
func NewProviderFromViper(v *viper.Viper, logger logrus.FieldLogger) (gostatsd.CloudProvider, error) {

	return &Provider{
		logger: logger,
	}, nil
}
