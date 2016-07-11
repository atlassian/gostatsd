package aws

import (
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
	max-retries = 5 # optional, default to 3

	# availability zone in which the server is located
	availability_zone = "us-west-2" # optional, will be retrieved from ec2 metadata if empty
`

// provider represents an aws provider.
type provider struct {
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
	return p.ec2Metadata, nil
}

type awsSDKProvider struct {
	config      *aws.Config
	ec2Metadata *ec2metadata.EC2Metadata
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
func (p *provider) Instance(IP types.IP) (*cloudTypes.Instance, error) {
	filters := []*ec2.Filter{newEc2Filter("private-ip-address", string(IP))}
	request := &ec2.DescribeInstancesInput{
		Filters: filters,
	}

	instances, err := p.ec2.DescribeInstances(request)
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
func (p *provider) ProviderName() string {
	return ProviderName
}

// SampleConfig returns the sample config for the datadog backend.
func (p *provider) SampleConfig() string {
	return sampleConfig
}

// SelfIP returns host's IPv4 address.
func (p *provider) SelfIP() (types.IP, error) {
	ip, err := p.metadata.GetMetadata("local-ipv4")
	return types.IP(ip), err
}

// Compute is an implementation of ec2 Compute.
func (p *awsSDKProvider) Compute(regionName string) (EC2, error) {
	service := ec2.New(session.New(p.config.Copy(&aws.Config{
		Region: aws.String(regionName),
	})))

	ec2 := &awsSdkEC2{
		ec2: service,
	}
	return ec2, nil
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

// NewProvider returns a new aws provider.
func NewProvider(awsServices Services, az string) (cloudTypes.Interface, error) {
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

	return &provider{availabilityZone: az, region: region, ec2: ec2, metadata: metadata}, nil
}

func newAWSSDKProvider(config *aws.Config, ec2Metadata *ec2metadata.EC2Metadata) (Services, error) {
	if config.HTTPClient != nil && config.HTTPClient.Timeout <= 0 {
		return nil, errors.New("http client timeout must be positive")
	}
	return &awsSDKProvider{
		config:      config,
		ec2Metadata: ec2Metadata,
	}, nil
}

// NewProviderFromViper returns a new aws provider.
func NewProviderFromViper(v *viper.Viper) (cloudTypes.Interface, error) {
	a := getSubViper(v, "aws")
	a.SetDefault("max_retries", 3)
	a.SetDefault("http_timeout", 3*time.Second)
	// This is the main config without credentials.
	config := &aws.Config{
		MaxRetries: aws.Int(a.GetInt("max_retries")),
		HTTPClient: &http.Client{
			Timeout: a.GetDuration("http_timeout"),
		},
	}
	ec2Metadata := ec2metadata.New(session.New(config))
	aws, err := newAWSSDKProvider(config.Copy(&aws.Config{
		Credentials: credentials.NewChainCredentials(
			[]credentials.Provider{
				&credentials.EnvProvider{},
				&ec2rolecreds.EC2RoleProvider{
					Client: ec2Metadata,
				},
				&credentials.SharedCredentialsProvider{},
			}),
	}), ec2Metadata)
	if err != nil {
		return nil, err
	}
	return NewProvider(aws, a.GetString("availability_zone"))
}

// Workaround https://github.com/spf13/viper/pull/165 and https://github.com/spf13/viper/issues/191
func getSubViper(v *viper.Viper, key string) *viper.Viper {
	var n *viper.Viper
	namespace := v.Get(key)
	if namespace != nil {
		n = v.Sub(key)
	}
	if n == nil {
		n = viper.New()
	}
	return n
}
