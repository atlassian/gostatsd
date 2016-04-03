package aws

import (
	"fmt"

	"github.com/jtblin/gostatsd/cloudprovider"
	"github.com/jtblin/gostatsd/types"

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
	providerName = "aws"
)

const sampleConfig = `
[aws]
	# maximum number of retries in case of retriable errors
	max-retries = 5 # optional, default to 3

	# availability zone in which the server is located
	availability_zone = "us-west-2" # optional, will be retrieved from ec2 metadata if empty
`

// Provider represents an aws provider.
type Provider struct {
	availabilityZone string
	ec2              EC2
	metadata         EC2Metadata
	region           string
}

// Services is an abstraction over AWS, to allow mocking/other implementations.
type Services interface {
	Compute(region string) (EC2, error)
	Metadata() (EC2Metadata, error)
}

// EC2 is an abstraction over EC2, to allow mocking/other implementations
// Note that the DescribeX functions return a list, so callers don't need to deal with paging.
type EC2 interface {
	// Query EC2 for instances matching the filter.
	DescribeInstances(request *ec2.DescribeInstancesInput) ([]*ec2.Instance, error)
}

// EC2Metadata is an abstraction over the AWS metadata service.
type EC2Metadata interface {
	// Query the EC2 metadata service (used to discover instance-id etc).
	GetMetadata(path string) (string, error)
}

// awsSdkEC2 is an implementation of the EC2 interface, backed by aws-sdk-go.
type awsSdkEC2 struct {
	ec2 *ec2.EC2
}

func (p *awsSDKProvider) Metadata() (EC2Metadata, error) {
	client := ec2metadata.New(session.New(&aws.Config{}))
	return client, nil
}

type awsSDKProvider struct {
	creds      *credentials.Credentials
	maxRetries int
}

func isNilOrEmpty(s *string) bool {
	return s == nil || *s == ""
}

// DescribeInstances is an implementation of EC2.Instances.
func (s *awsSdkEC2) DescribeInstances(request *ec2.DescribeInstancesInput) ([]*ec2.Instance, error) {
	// Instances are paged
	results := []*ec2.Instance{}
	var nextToken *string

	for {
		response, err := s.ec2.DescribeInstances(request)
		if err != nil {
			return nil, fmt.Errorf("error listing AWS instances: %v", err)
		}

		for _, reservation := range response.Reservations {
			results = append(results, reservation.Instances...)
		}

		nextToken = response.NextToken
		if isNilOrEmpty(nextToken) {
			break
		}
		request.NextToken = nextToken
	}

	return results, nil
}

func newEc2Filter(name string, value string) *ec2.Filter {
	filter := &ec2.Filter{
		Name: aws.String(name),
		Values: []*string{
			aws.String(value),
		},
	}
	return filter
}

// Instance returns the instance details from aws.
func (p *Provider) Instance(IP string) (*types.Instance, error) {
	filters := []*ec2.Filter{newEc2Filter("private-ip-address", IP)}
	request := &ec2.DescribeInstancesInput{
		Filters: filters,
	}

	instances, err := p.ec2.DescribeInstances(request)
	if err != nil {
		return nil, err
	}
	if len(instances) == 0 {
		return nil, fmt.Errorf("no instances returned")
	}

	i := instances[0]
	region, err := azToRegion(aws.StringValue(i.Placement.AvailabilityZone))
	if err != nil {
		log.Errorf("Error getting instance region: %v", err)
	}
	instance := &types.Instance{ID: aws.StringValue(i.InstanceId), Region: region}
	for _, tag := range i.Tags {
		instance.Tags = append(instance.Tags, fmt.Sprintf("%s:%s",
			types.NormalizeTagElement(aws.StringValue(tag.Key)), types.NormalizeTagElement(aws.StringValue(tag.Value))))
	}
	return instance, nil
}

// ProviderName returns the name of the provider.
func (p *Provider) ProviderName() string {
	return providerName
}

// SampleConfig returns the sample config for the datadog backend.
func (p *Provider) SampleConfig() string {
	return sampleConfig
}

// Compute is an implementation of ec2 Compute.
func (p *awsSDKProvider) Compute(regionName string) (EC2, error) {
	service := ec2.New(session.New(&aws.Config{
		Region:      &regionName,
		Credentials: p.creds,
		MaxRetries:  aws.Int(p.maxRetries),
	}))

	ec2 := &awsSdkEC2{
		ec2: service,
	}
	return ec2, nil
}

// Derives the region from a valid az name.
// Returns an error if the az is known invalid (empty).
func azToRegion(az string) (string, error) {
	if len(az) < 1 {
		return "", fmt.Errorf("invalid (empty) AZ")
	}
	region := az[:len(az)-1]
	return region, nil
}

// NewProvider returns a new aws provider.
func NewProvider(awsServices Services, az string) (p *Provider, err error) {
	metadata, err := awsServices.Metadata()
	if err != nil {
		return nil, fmt.Errorf("error creating AWS metadata client: %v", err)
	}

	if az == "" {
		if az, err = metadata.GetMetadata("placement/availability-zone"); err != nil {
			return nil, fmt.Errorf("error getting availability zone: %v", err)
		}
	}
	region, err := azToRegion(az)
	if err != nil {
		return nil, fmt.Errorf("error getting aws region: %v", err)
	}

	ec2, err := awsServices.Compute(region)
	if err != nil {
		return nil, fmt.Errorf("error creating AWS EC2 client: %v", err)
	}

	return &Provider{availabilityZone: az, region: region, ec2: ec2, metadata: metadata}, nil
}

func newAWSSDKProvider(creds *credentials.Credentials, maxRetries int) *awsSDKProvider {
	return &awsSDKProvider{creds: creds, maxRetries: maxRetries}
}

func init() {
	cloudprovider.RegisterCloudProvider(providerName, func(v *viper.Viper) (cloudprovider.Interface, error) {
		v.SetDefault("aws.max_retries", 3)
		creds := credentials.NewChainCredentials(
			[]credentials.Provider{
				&credentials.EnvProvider{},
				&ec2rolecreds.EC2RoleProvider{
					Client: ec2metadata.New(session.New(&aws.Config{})),
				},
				&credentials.SharedCredentialsProvider{},
			})
		aws := newAWSSDKProvider(creds, v.GetInt("aws.max_retries"))
		return NewProvider(aws, v.GetString("aws.availability_zone"))
	})
}
