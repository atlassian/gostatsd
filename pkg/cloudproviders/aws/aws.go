package aws

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/atlassian/gostatsd"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/spf13/viper"
	"golang.org/x/net/http2"
)

const (
	// ProviderName is the name of AWS cloud provider.
	ProviderName         = "aws"
	defaultClientTimeout = 9 * time.Second
)

// Provider represents an AWS provider.
type Provider struct {
	Metadata *ec2metadata.EC2Metadata
	Ec2      *ec2.EC2
}

// Instance returns instances details from AWS.
// ip -> nil pointer if instance was not found.
// map is returned even in case of errors because it may contain partial data.
func (p *Provider) Instance(ctx context.Context, IP ...gostatsd.IP) (map[gostatsd.IP]*gostatsd.Instance, error) {
	instances := make(map[gostatsd.IP]*gostatsd.Instance, len(IP))
	values := make([]*string, len(IP))
	for i, ip := range IP {
		instances[ip] = nil // initialize map. Used for lookups to see if info for IP was requested
		values[i] = aws.String(string(ip))
	}
	req, _ := p.Ec2.DescribeInstancesRequest(&ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("private-ip-address"),
				Values: values,
			},
		},
	})
	req.HTTPRequest = req.HTTPRequest.WithContext(ctx)
	err := req.EachPage(func(data interface{}, isLastPage bool) bool {
		for _, reservation := range data.(*ec2.DescribeInstancesOutput).Reservations {
			for _, instance := range reservation.Instances {
				ip := getInterestingInstanceIP(instance, instances)
				if ip == gostatsd.UnknownIP {
					log.Warnf("AWS returned unexpected EC2 instance: %#v", instance)
					continue
				}
				region, err := azToRegion(aws.StringValue(instance.Placement.AvailabilityZone))
				if err != nil {
					log.Errorf("Error getting instance region: %v", err)
				}
				tags := make(gostatsd.Tags, len(instance.Tags))
				for idx, tag := range instance.Tags {
					tags[idx] = fmt.Sprintf("%s:%s",
						gostatsd.NormalizeTagKey(aws.StringValue(tag.Key)),
						aws.StringValue(tag.Value))
				}
				instances[ip] = &gostatsd.Instance{
					ID:     aws.StringValue(instance.InstanceId),
					Region: region,
					Tags:   tags,
				}
			}
		}
		return true
	})
	if err != nil {
		// Avoid spamming logs if instance id is not visible yet due to eventual consistency.
		// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html#CommonErrors
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "InvalidInstanceID.NotFound" {
			return instances, nil
		}
		return instances, fmt.Errorf("error listing AWS instances: %v", err)
	}
	return instances, nil
}

func getInterestingInstanceIP(instance *ec2.Instance, instances map[gostatsd.IP]*gostatsd.Instance) gostatsd.IP {
	// Check primary private IPv4 address
	ip := gostatsd.IP(aws.StringValue(instance.PrivateIpAddress))
	if _, ok := instances[ip]; ok {
		return ip
	}
	// Check interfaces
	for _, iface := range instance.NetworkInterfaces {
		// Check private IPv4 addresses on interface
		for _, privateIP := range iface.PrivateIpAddresses {
			ip = gostatsd.IP(aws.StringValue(privateIP.PrivateIpAddress))
			if _, ok := instances[ip]; ok {
				return ip
			}
		}
		// Check private IPv6 addresses on interface
		for _, IPv6 := range iface.Ipv6Addresses {
			ip = gostatsd.IP(aws.StringValue(IPv6.Ipv6Address))
			if _, ok := instances[ip]; ok {
				return ip
			}
		}
	}
	return gostatsd.UnknownIP
}

// Name returns the name of the provider.
func (p *Provider) Name() string {
	return ProviderName
}

// SelfIP returns host's IPv4 address.
func (p *Provider) SelfIP() (gostatsd.IP, error) {
	ip, err := p.Metadata.GetMetadata("local-ipv4")
	return gostatsd.IP(ip), err
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
func NewProviderFromViper(v *viper.Viper) (gostatsd.CloudProvider, error) {
	a := getSubViper(v, "aws")
	a.SetDefault("max_retries", 3)
	a.SetDefault("client_timeout", defaultClientTimeout)
	httpTimeout := a.GetDuration("client_timeout")
	if httpTimeout <= 0 {
		return nil, errors.New("client timeout must be positive")
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
		WithCredentials(credentials.NewChainCredentials(
			[]credentials.Provider{
				&credentials.EnvProvider{},
				&ec2rolecreds.EC2RoleProvider{
					Client: metadata,
				},
				&credentials.SharedCredentialsProvider{},
			})).
		WithRegion(region)
	ec2Session, err := session.NewSession(ec2config)
	if err != nil {
		return nil, fmt.Errorf("error creating a new EC2 session: %v", err)
	}
	return &Provider{
		Metadata: metadata,
		Ec2:      ec2.New(ec2Session),
	}, nil
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	return n
}
