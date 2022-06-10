package aws

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/net/http2"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/util"
	"github.com/atlassian/gostatsd/pkg/stats"
)

type LocalIPMode int

const (
	Deny LocalIPMode = iota
	Allow
)

func NewLocalIPMode(str string) LocalIPMode {
	s := strings.ToLower(str)
	switch s {
	case "deny":
		return Deny
	case "allow":
		return Allow
	}
	return Deny
}

var (
	defaultLocalIPWhitelist = []string{
		"127.0.0.1",
		"localhost",
		"172.17.0.1", // docker gateway
	}
)

const (
	// ProviderName is the name of AWS cloud provider.
	ProviderName             = "aws"
	defaultClientTimeout     = 9 * time.Second
	defaultMaxInstancesBatch = 32
	defaultLocalIPMode       = Deny
)

// Provider represents an AWS provider.
type Provider struct {
	describeInstanceCount     uint64 // The cumulative number of times DescribeInstancesPagesWithContext has been called
	describeInstanceInstances uint64 // The cumulative number of instances which have been fed in to DescribeInstancesPagesWithContext
	describeInstancePages     uint64 // The cumulative number of pages from DescribeInstancesPagesWithContext
	describeInstanceErrors    uint64 // The cumulative number of errors seen from DescribeInstancesPagesWithContext
	describeInstanceFound     uint64 // The cumulative number of instances successfully found via DescribeInstancesPagesWithContext

	logger logrus.FieldLogger

	Metadata         *ec2metadata.EC2Metadata
	Ec2              *ec2.EC2
	MaxInstances     int
	localIPMode      LocalIPMode
	localIPWhitelist []string
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
	ips := make([]*string, len(IP))
	errors := make([]error, 0, 2)
	n := 0
	lookupLocal := false
	for _, ip := range IP {
		instances[ip] = nil // initialize map. Used for lookups to see if info for IP was requested
		if !p.isLocalIP(ip) {
			ips[n] = aws.String(string(ip))
			n = n + 1
		} else {
			lookupLocal = true
		}
	}

	values := ips[:n]
	input := &ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("private-ip-address"),
				Values: values,
			},
		},
	}

	atomic.AddUint64(&p.describeInstanceCount, 1)
	atomic.AddUint64(&p.describeInstanceInstances, uint64(len(values)))
	instancesFound := uint64(0)
	pages := uint64(0)

	p.logger.WithField("ips", IP).Debug("Looking up instances")
	err := p.Ec2.DescribeInstancesPagesWithContext(ctx, input, func(page *ec2.DescribeInstancesOutput, lastPage bool) bool {
		pages++
		for _, reservation := range page.Reservations {
			for _, instance := range reservation.Instances {
				ip := getInterestingInstanceIP(instance, instances)
				if ip == gostatsd.UnknownSource {
					p.logger.Warnf("AWS returned unexpected EC2 instance: %#v", instance)
					continue
				}
				instancesFound++
				instances[ip] = p.gostatsdInstanceFromInstance(ip, instance)
			}
		}
		return true
	})

	atomic.AddUint64(&p.describeInstancePages, pages)
	atomic.AddUint64(&p.describeInstanceFound, instancesFound)

	if err != nil {
		atomic.AddUint64(&p.describeInstanceErrors, 1)

		// Avoid spamming logs if instance id is not visible yet due to eventual consistency.
		// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html#CommonErrors
		if !isEventualConsistencyErr(err) {
			errors = append(errors, fmt.Errorf("error listing AWS instances: %v", err))
		}
	}

	var localInstance *gostatsd.Instance

	if lookupLocal {
		localInstance, err = p.instanceFromMetadata(ctx)
		if err != nil && !isEventualConsistencyErr(err) {
			errors = append(errors, fmt.Errorf("error inspecting local instance: %v", err))
		}
	}

	for ip, instance := range instances {
		if instance == nil {
			if localInstance != nil && p.isLocalIP(ip) {
				p.logger.WithField("ip", ip).Debugf("Using local instance for IP %v", ip)
				instances[ip] = localInstance
			} else {
				p.logger.WithField("ip", ip).Debug("No results looking up instance")
			}
		}
	}

	if len(errors) > 0 {
		return instances, multiError(errors)
	}

	return instances, nil
}

func getInterestingInstanceIP(instance *ec2.Instance, instances map[gostatsd.Source]*gostatsd.Instance) gostatsd.Source {
	// Check primary private IPv4 address
	ip := gostatsd.Source(aws.StringValue(instance.PrivateIpAddress))
	if _, ok := instances[ip]; ok {
		return ip
	}
	// Check interfaces
	for _, iface := range instance.NetworkInterfaces {
		// Check private IPv4 addresses on interface
		for _, privateIP := range iface.PrivateIpAddresses {
			ip = gostatsd.Source(aws.StringValue(privateIP.PrivateIpAddress))
			if _, ok := instances[ip]; ok {
				return ip
			}
		}
		// Check private IPv6 addresses on interface
		for _, IPv6 := range iface.Ipv6Addresses {
			ip = gostatsd.Source(aws.StringValue(IPv6.Ipv6Address))
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

func (p *Provider) instanceFromMetadata(ctx context.Context) (*gostatsd.Instance, error) {
	identityDoc, err := p.Metadata.GetInstanceIdentityDocument()
	if err != nil {
		return nil, err
	}

	values := []*string{&identityDoc.InstanceID}

	input := &ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("instance-id"),
				Values: values,
			},
		},
	}

	atomic.AddUint64(&p.describeInstanceCount, 1)
	atomic.AddUint64(&p.describeInstanceInstances, 1)

	var cachedInstance *gostatsd.Instance

	p.logger.Debugf("Looking up instance for local instance ID %v", identityDoc.InstanceID)
	err = p.Ec2.DescribeInstancesPagesWithContext(ctx, input, func(page *ec2.DescribeInstancesOutput, lastPage bool) bool {
		reservationCount := len(page.Reservations)
		if reservationCount > 0 {
			if reservationCount > 1 {
				p.logger.WithFields(logrus.Fields{
					"instance": identityDoc.InstanceID,
				}).Warnf("Found more than one reservation for local instance ID %v. Using first.", identityDoc.InstanceID)
			}

			reservation := page.Reservations[0]
			instanceCount := len(reservation.Instances)
			if instanceCount > 0 {
				if instanceCount > 1 {
					p.logger.WithFields(logrus.Fields{
						"instance":      identityDoc.InstanceID,
						"reservationId": reservation.ReservationId,
					}).Warnf("Found more than one instance for local instance ID %v and reservation ID %v. Using first.", identityDoc.InstanceID, reservation.ReservationId)
				}

				cachedInstance = p.gostatsdInstanceFromInstance(gostatsd.Source(identityDoc.PrivateIP), reservation.Instances[0])
			}
		}
		return false
	})

	atomic.AddUint64(&p.describeInstancePages, 1)
	if cachedInstance != nil {
		atomic.AddUint64(&p.describeInstanceFound, 1)
	}

	if err != nil {
		atomic.AddUint64(&p.describeInstanceErrors, 1)

		return nil, err
	}

	return cachedInstance, nil
}

func (p *Provider) gostatsdInstanceFromInstance(ip gostatsd.Source, instance *ec2.Instance) *gostatsd.Instance {
	region, err := azToRegion(aws.StringValue(instance.Placement.AvailabilityZone))
	if err != nil {
		p.logger.Errorf("Error getting instance region: %v", err)
	}
	tags := make(gostatsd.Tags, len(instance.Tags)+1)
	for idx, tag := range instance.Tags {
		tags[idx] = fmt.Sprintf("%s:%s",
			gostatsd.NormalizeTagKey(aws.StringValue(tag.Key)),
			aws.StringValue(tag.Value))
	}
	tags[len(tags)-1] = "region:" + region

	p.logger.WithFields(logrus.Fields{
		"instance": instance.InstanceId,
		"ip":       ip,
		"tags":     tags,
	}).Debug("Added tags")

	return &gostatsd.Instance{
		ID:   gostatsd.Source(aws.StringValue(instance.InstanceId)),
		Tags: tags,
	}
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

func isEventualConsistencyErr(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "InvalidInstanceID.NotFound" {
		return true
	}
	return false
}

func multiError(errors []error) error {
	errs := make([]string, 0, len(errors)+1)
	errs = append(errs, fmt.Sprintf("%d errors occurred", len(errors)))
	for _, err := range errors {
		errs = append(errs, err.Error())
	}
	return fmt.Errorf(strings.Join(errs, ", "))
}

func (p *Provider) isLocalIP(ip gostatsd.Source) bool {
	if p.localIPMode == Deny {
		return false
	}

	return contains(p.localIPWhitelist, string(ip))
}

// contains checks if item is within slice
func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// NewProviderFromViper returns a new aws provider.
func NewProviderFromViper(v *viper.Viper, logger logrus.FieldLogger, _ string) (gostatsd.CloudProvider, error) {
	a := util.GetSubViper(v, "aws")
	a.SetDefault("max_retries", 3)
	a.SetDefault("client_timeout", defaultClientTimeout)
	a.SetDefault("max_instances_batch", defaultMaxInstancesBatch)
	a.SetDefault("local_ip_mode", defaultLocalIPMode)
	a.SetDefault("local_ip_whitelist", defaultLocalIPWhitelist)
	httpTimeout := a.GetDuration("client_timeout")
	if httpTimeout <= 0 {
		return nil, errors.New("client timeout must be positive")
	}
	maxInstances := a.GetInt("max_instances_batch")
	if maxInstances <= 0 {
		return nil, errors.New("max number of instances per batch must be positive")
	}
	localIPMode := NewLocalIPMode(a.GetString("local_ip_mode"))

	var localIPWhitelist []string

	if localIPMode == Allow {
		localIPWhitelist = a.GetStringSlice("local_ip_whitelist")
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
	sharedConfig := aws.NewConfig().
		WithHTTPClient(&http.Client{
			Transport: transport,
			Timeout:   httpTimeout,
		}).
		WithMaxRetries(a.GetInt("max_retries"))
	metadataSession, err := session.NewSession(sharedConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating a new Metadata session: %v", err)
	}
	metadata := ec2metadata.New(metadataSession)
	region, err := metadata.Region()
	if err != nil {
		return nil, fmt.Errorf("error getting AWS region: %v", err)
	}
	ec2config := sharedConfig.Copy().
		WithRegion(region)
	ec2Session, err := session.NewSession(ec2config)
	if err != nil {
		return nil, fmt.Errorf("error creating a new EC2 session: %v", err)
	}
	return &Provider{
		Metadata:         metadata,
		Ec2:              ec2.New(ec2Session),
		MaxInstances:     maxInstances,
		logger:           logger,
		localIPMode:      localIPMode,
		localIPWhitelist: localIPWhitelist,
	}, nil
}
