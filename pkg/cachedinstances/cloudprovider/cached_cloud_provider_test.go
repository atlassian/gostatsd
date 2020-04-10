package cloudprovider

import (
	"context"
	"testing"
	"time"

	"github.com/ash2k/stager/wait"
	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/cloudproviders/fakeprovider"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestCachedCloudProviderExpirationAndRefresh(t *testing.T) {
	// These still use a real clock, which means they're more susceptible to
	// CPU load triggering a race condition, therefore there's no t.Parallel()
	fp := &fakeprovider.IP{}
	/*
		Note: lookup reads in to a batch for up to 10ms

		T+0: IP is sent to lookup
		T+10: lookup is performed, cached with eviction time = T+10+50=60, refresh time = T+10+10=20
		T+11: refresh loop, nothing to do
		T+20: cache entry passes refresh time
		T+22: refresh loop, cache item is dispatched for refreshing
		T+32: cache lookup is performed, eviction time is unchanged, refresh time = T+32+10=42
		T+33: refresh loop, nothing to do
		T+42: cache entry passes refresh time
		T+44: refresh loop, cache item is dispatched for refreshing
		T+54: cache lookup is performed, eviction time is unchanged, refresh time = T+54+10=64
		T+55: refresh loop, nothing to do
		T+60: cache entry passes expiry time
		T+64: cache entry passes refresh time
		T+66: refresh loop, entry is expired
		T+70: sleep completes
	*/
	ci := NewCachedCloudProvider(logrus.StandardLogger(), rate.NewLimiter(100, 120), fp, gostatsd.CacheOptions{
		CacheRefreshPeriod:        11 * time.Millisecond,
		CacheEvictAfterIdlePeriod: 50 * time.Millisecond,
		CacheTTL:                  10 * time.Millisecond,
		CacheNegativeTTL:          100 * time.Millisecond,
	})
	var wg wait.Group
	defer wg.Wait()
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	wg.StartWithContext(ctx, ci.Run)
	const peekIp gostatsd.IP = "1.2.3.4"
	instance, exists := ci.Peek(peekIp)
	require.Nil(t, instance)
	require.False(t, exists)
	ci.IpSink() <- peekIp
	time.Sleep(70 * time.Millisecond) // Should be refreshed couple of times and evicted.

	cancelFunc()
	wg.Wait()

	// Cache might refresh multiple times, ensure it only refreshed with the expected IP
	for _, ip := range fp.IPs() {
		assert.Equal(t, peekIp, ip)
	}
	assert.GreaterOrEqual(t, len(fp.IPs()), 2) // Ensure it does at least 1 lookup + 1 refresh
	assert.Zero(t, len(ci.cache))              // Ensure it eventually expired
}
