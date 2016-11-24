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
	ProviderName = "aws"
)

const sampleConfig = `
[aws]
	# maximum number of retries in case of retriable errors
	max_retries = 5 # optional, default to 3
`

// Provider represents an AWS provider.
type Provider struct {
	Metadata *ec2metadata.EC2Metadata
	Ec2      *ec2.EC2
}

func newEc2Filter(name string, value string) *ec2.Filter {
	return &ec2.Filter{
		Name: aws.String(name),
		Values: []*string{
			aws.String(value),
		},
	}
}

// Instance returns the instance details from aws.
func (p *Provider) Instance(ctx context.Context, IP gostatsd.IP) (*gostatsd.Instance, error) {
	req, _ := p.Ec2.DescribeInstancesRequest(&ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			newEc2Filter("private-ip-address", string(IP)),
		},
	})
	req.HTTPRequest = req.HTTPRequest.WithContext(ctx)
	var inst *ec2.Instance
	err := req.EachPage(func(data interface{}, isLastPage bool) bool {
		for _, reservation := range data.(*ec2.DescribeInstancesOutput).Reservations {
			for _, instance := range reservation.Instances {
				inst = instance
				return false
			}
		}
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("error listing AWS instances: %v", err)
	}
	if inst == nil {
		return nil, errors.New("no instances found")
	}
	region, err := azToRegion(aws.StringValue(inst.Placement.AvailabilityZone))
	if err != nil {
		log.Errorf("Error getting instance region: %v", err)
	}
	tags := make(gostatsd.Tags, len(inst.Tags))
	for idx, tag := range inst.Tags {
		tags[idx] = fmt.Sprintf("%s:%s",
			gostatsd.NormalizeTagKey(aws.StringValue(tag.Key)),
			aws.StringValue(tag.Value))
	}
	instance := &gostatsd.Instance{
		ID:     aws.StringValue(inst.InstanceId),
		Region: region,
		Tags:   tags,
	}
	return instance, nil
}

// ProviderName returns the name of the provider.
func (p *Provider) Name() string {
	return ProviderName
}

// SampleConfig returns the sample config for the datadog backend.
func (p *Provider) SampleConfig() string {
	return sampleConfig
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
	a.SetDefault("http_timeout", 3*time.Second)
	httpTimeout := a.GetDuration("http_timeout")
	if httpTimeout <= 0 {
		return nil, errors.New("http client timeout must be positive")
	}

	// This is the main config without credentials.
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSHandshakeTimeout: 5 * time.Second,
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
	}
	if err := http2.ConfigureTransport(transport); err != nil {
		return nil, err
	}
	config := &aws.Config{
		MaxRetries: aws.Int(a.GetInt("max_retries")),
		HTTPClient: &http.Client{
			Transport: transport,
			Timeout:   httpTimeout,
		},
	}
	metadata := ec2metadata.New(session.New(config))
	az, err := metadata.GetMetadata("placement/availability-zone")
	if err != nil {
		return nil, fmt.Errorf("error getting availability zone: %v", err)
	}
	region, err := azToRegion(az)
	if err != nil {
		return nil, fmt.Errorf("error getting aws region: %v", err)
	}
	return &Provider{
		Metadata: metadata,
		Ec2: ec2.New(session.New(config.Copy(&aws.Config{
			Credentials: credentials.NewChainCredentials(
				[]credentials.Provider{
					&credentials.EnvProvider{},
					&ec2rolecreds.EC2RoleProvider{
						Client: metadata,
					},
					&credentials.SharedCredentialsProvider{},
				}),
			Region: aws.String(region),
		}))),
	}, nil
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	return n
}
