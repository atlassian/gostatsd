package aws

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2Types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/net/http2"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/util"
	"github.com/atlassian/gostatsd/pkg/stats"
)

const (
	// ProviderName is the name of AWS cloud provider.
	ProviderName             = "aws"
	defaultClientTimeout     = 9 * time.Second
	defaultMaxInstancesBatch = 32
)

// Provider represents an AWS provider.
type Provider struct {
	describeInstanceCount     uint64 // The cumulative number of times DescribeInstancesPagesWithContext has been called
	describeInstanceInstances uint64 // The cumulative number of instances which have been fed in to DescribeInstancesPagesWithContext
	describeInstancePages     uint64 // The cumulative number of pages from DescribeInstancesPagesWithContext
	describeInstanceErrors    uint64 // The cumulative number of errors seen from DescribeInstancesPagesWithContext
	describeInstanceFound     uint64 // The cumulative number of instances successfully found via DescribeInstancesPagesWithContext

	logger logrus.FieldLogger

	Ec2          ec2.DescribeInstancesAPIClient
	MaxInstances int
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
			statser.Gauge("cloudprovider.aws.describeinstancecount", float64(atomic.LoadUint64(&p.describeInstanceCount)), nil)
			statser.Gauge("cloudprovider.aws.describeinstanceinstances", float64(atomic.LoadUint64(&p.describeInstanceInstances)), nil)
			statser.Gauge("cloudprovider.aws.describeinstancepages", float64(atomic.LoadUint64(&p.describeInstancePages)), nil)
			statser.Gauge("cloudprovider.aws.describeinstanceerrors", float64(atomic.LoadUint64(&p.describeInstanceErrors)), nil)
			statser.Gauge("cloudprovider.aws.describeinstancefound", float64(atomic.LoadUint64(&p.describeInstanceFound)), nil)
		}
	}
}

// Instance returns instances details from AWS.
// ip -> nil pointer if instance was not found.
// map is returned even in case of errors because it may contain partial data.
func (p *Provider) Instance(ctx context.Context, IP ...gostatsd.Source) (map[gostatsd.Source]*gostatsd.Instance, error) {
	instances := make(map[gostatsd.Source]*gostatsd.Instance, len(IP))
	values := make([]string, len(IP))
	for i, ip := range IP {
		instances[ip] = nil // initialize map. Used for lookups to see if info for IP was requested
		values[i] = string(ip)
	}
	privateIPFilter := "private-ip-address"
	inputFilters := []ec2Types.Filter{
		{
			Name:   &privateIPFilter,
			Values: values,
		},
	}

	atomic.AddUint64(&p.describeInstanceCount, 1)
	atomic.AddUint64(&p.describeInstanceInstances, uint64(len(IP)))
	instancesFound := uint64(0)
	pages := uint64(0)
	var err error
	input := &ec2.DescribeInstancesInput{
		Filters: inputFilters,
	}

	p.logger.WithField("ips", IP).Debug("Looking up instances")
	paginator := ec2.NewDescribeInstancesPaginator(p.Ec2, input)
	for paginator.HasMorePages() {
		pages++

		page, rawErr := paginator.NextPage(ctx)
		if rawErr != nil {
			atomic.AddUint64(&p.describeInstanceErrors, 1)

			if rawErr.Error() != "InvalidInstanceID.NotFound" {
				err = fmt.Errorf("error listing AWS instances: %v", rawErr)
			}
			break
		}
		for _, reservation := range page.Reservations {
			for _, instance := range reservation.Instances {
				ip := getInterestingInstanceIP(instance, instances)
				if ip == gostatsd.UnknownSource {
					p.logger.Warnf("AWS returned unexpected EC2 instance: %#v", instance)
					continue
				}
				instancesFound++
				region, err := azToRegion(*instance.Placement.AvailabilityZone)
				if err != nil {
					p.logger.Errorf("Error getting instance region: %v", err)
				}
				tags := make(gostatsd.Tags, len(instance.Tags)+1)
				for idx, tag := range instance.Tags {
					tags[idx] = fmt.Sprintf("%s:%s",
						gostatsd.NormalizeTagKey(*tag.Key),
						*tag.Value)
				}
				tags[len(tags)-1] = "region:" + region
				instances[ip] = &gostatsd.Instance{
					ID:   gostatsd.Source(*instance.InstanceId),
					Tags: tags,
				}
				p.logger.WithFields(logrus.Fields{
					"instance": instance.InstanceId,
					"ip":       ip,
					"tags":     tags,
				}).Debug("Added tags")
			}
		}
	}

	atomic.AddUint64(&p.describeInstancePages, pages)
	atomic.AddUint64(&p.describeInstanceFound, instancesFound)

	for ip, instance := range instances {
		if instance == nil {
			p.logger.WithField("ip", ip).Debug("No results looking up instance")
		}
	}
	return instances, err
}

func getInterestingInstanceIP(instance ec2Types.Instance, instances map[gostatsd.Source]*gostatsd.Instance) gostatsd.Source {
	// Check primary private IPv4 address
	ip := gostatsd.Source(*instance.PrivateIpAddress)
	if _, ok := instances[ip]; ok {
		return ip
	}
	// Check interfaces
	for _, iface := range instance.NetworkInterfaces {
		// Check private IPv4 addresses on interface
		for _, privateIP := range iface.PrivateIpAddresses {
			ip = gostatsd.Source(*privateIP.PrivateIpAddress)
			if _, ok := instances[ip]; ok {
				return ip
			}
		}
		// Check private IPv6 addresses on interface
		for _, IPv6 := range iface.Ipv6Addresses {
			ip = gostatsd.Source(*IPv6.Ipv6Address)
			if _, ok := instances[ip]; ok {
				return ip
			}
		}
	}
	return gostatsd.UnknownSource
}

// MaxInstancesBatch returns maximum number of instances that could be requested via the Instance method.
func (p *Provider) MaxInstancesBatch() int {
	return p.MaxInstances
}

// Name returns the name of the provider.
func (p *Provider) Name() string {
	return ProviderName
}

// Derives the region from a valid az name.
// Returns an error if the az is known invalid (empty).
func azToRegion(az string) (string, error) {
	if az == "" {
		return "", errors.New("invalid (empty) AZ")
	}
	region := az[:len(az)-1]
	return region, nil
}

// NewProviderFromViper returns a new aws provider.
func NewProviderFromViper(v *viper.Viper, logger logrus.FieldLogger, _ string) (gostatsd.CloudProvider, error) {
	a := util.GetSubViper(v, "aws")
	a.SetDefault("max_retries", 3)
	a.SetDefault("client_timeout", defaultClientTimeout)
	a.SetDefault("max_instances_batch", defaultMaxInstancesBatch)
	httpTimeout := a.GetDuration("client_timeout")
	if httpTimeout <= 0 {
		return nil, errors.New("client timeout must be positive")
	}
	maxInstances := a.GetInt("max_instances_batch")
	if maxInstances <= 0 {
		return nil, errors.New("max number of instances per batch must be positive")
	}

	// This is the main config without credentials.
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSHandshakeTimeout: 3 * time.Second,
		TLSClientConfig: &tls.Config{
			// Can't use SSLv3 because of POODLE and BEAST
			// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
			// Can't use TLSv1.1 because of RC4 cipher usage
			MinVersion: tls.VersionTLS12,
		},
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:    50,
		IdleConnTimeout: 1 * time.Minute,
	}
	if err := http2.ConfigureTransport(transport); err != nil {
		return nil, err
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithHTTPClient(&http.Client{
			Transport: transport,
			Timeout:   httpTimeout,
		}),
		config.WithRetryMaxAttempts(a.GetInt("max_retries")),
	)

	metadataClient := imds.NewFromConfig(cfg)

	region, err := metadataClient.GetRegion(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("error getting AWS region: %v", err)
	}

	ec2config, err := config.LoadDefaultConfig(context.Background(),
		config.WithHTTPClient(&http.Client{
			Transport: transport,
			Timeout:   httpTimeout,
		}),
		config.WithRetryMaxAttempts(a.GetInt("max_retries")),
		config.WithRegion(region.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating a new EC2 session: %v", err)
	}
	return &Provider{
		Ec2:          ec2.NewFromConfig(ec2config),
		MaxInstances: maxInstances,
		logger:       logger,
	}, nil
}
