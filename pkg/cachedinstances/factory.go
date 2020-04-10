package cachedinstances

import (
	"fmt"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/cachedinstances/cloudprovider"
	"github.com/atlassian/gostatsd/pkg/cloudproviders/aws"
	"github.com/atlassian/gostatsd/pkg/cloudproviders/k8s"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

var (
	// defaultCloudProviderCacheValues contains the default cache values for each cloud provider.
	defaultCloudProviderCacheValues = map[string]gostatsd.CacheOptions{
		k8s.ProviderName: {
			// The refresh period is low for k8s provider since there is no network cost to looking up pods
			CacheRefreshPeriod:        15 * time.Second,
			CacheEvictAfterIdlePeriod: gostatsd.DefaultCacheEvictAfterIdlePeriod,
			// TTLs are low for the k8s provider because there is no network cost to doing a refresh of the data
			// This means we get fresh data every time the refresh period is up
			CacheTTL:         1 * time.Millisecond,
			CacheNegativeTTL: 1 * time.Millisecond,
		},
		aws.ProviderName: {
			CacheRefreshPeriod:        gostatsd.DefaultCacheRefreshPeriod,
			CacheEvictAfterIdlePeriod: gostatsd.DefaultCacheEvictAfterIdlePeriod,
			CacheTTL:                  gostatsd.DefaultCacheTTL,
			CacheNegativeTTL:          gostatsd.DefaultCacheNegativeTTL,
		},
	}

	// defaultCloudProviderLimiterValues contains the default limiter values for each cloud provider.
	defaultCloudProviderLimiterValues = map[string]limiterValues{
		k8s.ProviderName: {
			// High limit for k8s since we don't make network requests for each data request
			maxCloudRequests:   10000,
			burstCloudRequests: 5000,
		},
		aws.ProviderName: {
			maxCloudRequests:   gostatsd.DefaultMaxCloudRequests,
			burstCloudRequests: gostatsd.DefaultBurstCloudRequests,
		},
	}
)

type limiterValues struct {
	maxCloudRequests   int
	burstCloudRequests int
}

// NewCachedInstancesFromViper initialises a new cached instances.
func NewCachedInstancesFromViper(logger logrus.FieldLogger, cloudProvider gostatsd.CloudProvider, v *viper.Viper) (gostatsd.CachedInstances, error) {
	// Cloud provider defaults
	cloudProviderName := cloudProvider.Name()
	cpDefaultCacheOpts, ok := defaultCloudProviderCacheValues[cloudProviderName]
	if !ok {
		return nil, fmt.Errorf("could not find default cache values for cloud provider %q", cloudProviderName)
	}
	cpDefaultLimiterOpts, ok := defaultCloudProviderLimiterValues[cloudProviderName]
	if !ok {
		return nil, fmt.Errorf("could not find default cache values for cloud provider %q", cloudProviderName)
	}

	// Set the defaults in Viper based on the cloud provider values before we manipulate things
	v.SetDefault(gostatsd.ParamCacheRefreshPeriod, cpDefaultCacheOpts.CacheRefreshPeriod)
	v.SetDefault(gostatsd.ParamCacheEvictAfterIdlePeriod, cpDefaultCacheOpts.CacheEvictAfterIdlePeriod)
	v.SetDefault(gostatsd.ParamCacheTTL, cpDefaultCacheOpts.CacheTTL)
	v.SetDefault(gostatsd.ParamCacheNegativeTTL, cpDefaultCacheOpts.CacheNegativeTTL)
	v.SetDefault(gostatsd.ParamMaxCloudRequests, cpDefaultLimiterOpts.maxCloudRequests)
	v.SetDefault(gostatsd.ParamBurstCloudRequests, cpDefaultLimiterOpts.burstCloudRequests)

	// Set the used values based on the defaults merged with any overrides
	cacheOptions := gostatsd.CacheOptions{
		CacheRefreshPeriod:        v.GetDuration(gostatsd.ParamCacheRefreshPeriod),
		CacheEvictAfterIdlePeriod: v.GetDuration(gostatsd.ParamCacheEvictAfterIdlePeriod),
		CacheTTL:                  v.GetDuration(gostatsd.ParamCacheTTL),
		CacheNegativeTTL:          v.GetDuration(gostatsd.ParamCacheNegativeTTL),
	}
	limiter := rate.NewLimiter(rate.Limit(v.GetInt(gostatsd.ParamMaxCloudRequests)), v.GetInt(gostatsd.ParamBurstCloudRequests))
	return cloudprovider.NewCachedCloudProvider(logger, limiter, cloudProvider, cacheOptions), nil
}
