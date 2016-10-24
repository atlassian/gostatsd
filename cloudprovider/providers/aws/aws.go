package aws

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	cloudTypes "github.com/atlassian/gostatsd/cloudprovider/types"
	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/spf13/viper"
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

// DescribeInstances is an implementation of EC2.Instances.
func (p *Provider) describeInstances(ctx context.Context, request *ec2.DescribeInstancesInput) ([]*ec2.Instance, error) {
	// Instances are paged
	results := []*ec2.Instance{}

	for {
		req, response := p.Ec2.DescribeInstancesRequest(request)
		req.HTTPRequest = req.HTTPRequest.WithContext(ctx)
		err := req.Send()
		if err != nil {
			return nil, fmt.Errorf("error listing AWS instances: %v", err)
		}

		for _, reservation := range response.Reservations {
			results = append(results, reservation.Instances...)
		}

		if response.NextToken == nil {
			break
		}
		request.NextToken = response.NextToken
	}

	return results, nil
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
func (p *Provider) Instance(ctx context.Context, IP types.IP) (*cloudTypes.Instance, error) {
	filters := []*ec2.Filter{newEc2Filter("private-ip-address", string(IP))}
	request := &ec2.DescribeInstancesInput{
		Filters: filters,
	}

	instances, err := p.describeInstances(ctx, request)
	if err != nil {
		return nil, err
	}
	if len(instances) == 0 {
		return nil, errors.New("no instances returned")
	}

	i := instances[0]
	region, err := azToRegion(aws.StringValue(i.Placement.AvailabilityZone))
	if err != nil {
		log.Errorf("Error getting instance region: %v", err)
	}
	tags := make(types.Tags, len(i.Tags))
	for idx, tag := range i.Tags {
		tags[idx] = fmt.Sprintf("%s:%s",
			types.NormalizeTagKey(aws.StringValue(tag.Key)),
			aws.StringValue(tag.Value))
	}
	instance := &cloudTypes.Instance{
		ID:     aws.StringValue(i.InstanceId),
		Region: region,
		Tags:   tags,
	}
	return instance, nil
}

// ProviderName returns the name of the provider.
func (p *Provider) ProviderName() string {
	return ProviderName
}

// SampleConfig returns the sample config for the datadog backend.
func (p *Provider) SampleConfig() string {
	return sampleConfig
}

// SelfIP returns host's IPv4 address.
func (p *Provider) SelfIP() (types.IP, error) {
	ip, err := p.Metadata.GetMetadata("local-ipv4")
	return types.IP(ip), err
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
func NewProviderFromViper(v *viper.Viper) (cloudTypes.Interface, error) {
	a := getSubViper(v, "aws")
	a.SetDefault("max_retries", 3)
	a.SetDefault("http_timeout", 3*time.Second)
	httpTimeout := a.GetDuration("http_timeout")
	if httpTimeout <= 0 {
		return nil, errors.New("http client timeout must be positive")
	}

	// This is the main config without credentials.
	config := &aws.Config{
		MaxRetries: aws.Int(a.GetInt("max_retries")),
		HTTPClient: &http.Client{
			Timeout: httpTimeout,
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
