package aws

import (
	"errors"
	"testing"

	ec2Types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"

	"github.com/atlassian/gostatsd"
)

func TestAzToRegion(t *testing.T) {
	testCases := []struct {
		value  string
		region string
		err    error
	}{
		{"", "", errors.New("invalid (empty) AZ")},
		{"us-east-1", "us-east-", nil},
	}
	for _, testCase := range testCases {
		region, err := azToRegion(testCase.value)
		if testCase.value != "" {
			assert.Equal(t, region, testCase.region)
		}
		assert.Equal(t, testCase.err, err)
	}
}

func TestGetInterestingInstanceIP(t *testing.T) {
	privateIP := "127.0.0.1"
	IPV4Address := "127.0.0.3"
	IPV6Address := "2001:db8::8a2e:370:7334"
	testCases := []struct {
		instance       ec2Types.Instance
		instances      map[gostatsd.Source]*gostatsd.Instance
		expectedResult gostatsd.Source
	}{
		{
			instance: ec2Types.Instance{
				PrivateIpAddress: &privateIP,
			},
			instances:      nil,
			expectedResult: gostatsd.UnknownSource,
		},
		{
			instance: ec2Types.Instance{
				PrivateIpAddress: &privateIP,
			},
			instances: map[gostatsd.Source]*gostatsd.Instance{
				gostatsd.Source(privateIP): {
					ID: "xxxxxxx",
					Tags: []string{
						"tag:xxxxxx",
					},
				},
			},
			expectedResult: gostatsd.Source(privateIP),
		},
		{
			instance: ec2Types.Instance{
				PrivateIpAddress: &privateIP,
				NetworkInterfaces: []ec2Types.InstanceNetworkInterface{
					{
						PrivateIpAddresses: []ec2Types.InstancePrivateIpAddress{
							{
								PrivateIpAddress: &IPV4Address,
							},
						},
					},
				},
			},
			instances: map[gostatsd.Source]*gostatsd.Instance{
				gostatsd.Source(IPV4Address): {
					ID: "xxxxxxx",
					Tags: []string{
						"tag:xxxxxx",
					},
				},
			},
			expectedResult: gostatsd.Source(IPV4Address),
		},
		{
			instance: ec2Types.Instance{
				PrivateIpAddress: &privateIP,
				NetworkInterfaces: []ec2Types.InstanceNetworkInterface{
					{
						Ipv6Addresses: []ec2Types.InstanceIpv6Address{
							{
								Ipv6Address: &IPV6Address,
							},
						},
					},
				},
			},
			instances: map[gostatsd.Source]*gostatsd.Instance{
				gostatsd.Source(IPV6Address): {
					ID: "xxxxxxx",
					Tags: []string{
						"tag:xxxxxx",
					},
				},
			},
			expectedResult: gostatsd.Source(IPV6Address),
		},
	}
	for _, testCase := range testCases {
		result := getInterestingInstanceIP(testCase.instance, testCase.instances)
		assert.Equal(t, testCase.expectedResult, result)
	}

}
