package statsd

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/cloudproviders/aws"
	"github.com/atlassian/gostatsd/pkg/cloudproviders/k8s"

	"github.com/ash2k/stager/wait"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

// BenchmarkCloudHandlerDispatchMetric is a benchmark intended to (manually) test
// the impact of the CloudHandler.statsCacheHit field.
func BenchmarkCloudHandlerDispatchMetric(b *testing.B) {
	fp := &fakeProviderIP{}
	nh := &nopHandler{}
	ch := NewCloudHandler(fp, nh, logrus.StandardLogger(), rate.NewLimiter(100, 120), CacheOptions{
		CacheRefreshPeriod:        100 * time.Millisecond,
		CacheEvictAfterIdlePeriod: 700 * time.Millisecond,
		CacheTTL:                  500 * time.Millisecond,
		CacheNegativeTTL:          500 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ch.Run(ctx)

	b.ReportAllocs()
	b.ResetTimer()

	ctxBackground := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := sm1()
			ch.DispatchMetrics(ctxBackground, []*gostatsd.Metric{&m})
		}
	})
}

func TestTransientInstanceFailure(t *testing.T) {
	t.Parallel()
	fpt := &fakeProviderTransient{
		failureMode: []int{
			0, // success, prime the cache
			1, // soft failure, data should remain in cache
		},
	}

	counting := &countingHandler{}

	ch := NewCloudHandler(fpt, counting, logrus.StandardLogger(), rate.NewLimiter(100, 120), CacheOptions{
		CacheRefreshPeriod:        50 * time.Millisecond,
		CacheEvictAfterIdlePeriod: 1 * time.Minute,
		CacheTTL:                  1 * time.Millisecond,
		CacheNegativeTTL:          1 * time.Millisecond,
	})

	// t+0: instance is queried, goes in cache
	// t+50ms: instance refreshed (failure)
	// t+100ms: instance is queried, must be in cache

	var wg wait.Group
	defer wg.Wait()
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	wg.StartWithContext(ctx, ch.Run)
	m1 := sm1()
	m2 := sm1()

	// t+0: prime the cache
	ch.DispatchMetrics(ctx, []*gostatsd.Metric{&m1})

	time.Sleep(50 * time.Millisecond)
	// t+50: refresh
	time.Sleep(50 * time.Millisecond)

	// t+100ms: read from cache, must still be valid
	ch.DispatchMetrics(ctx, []*gostatsd.Metric{&m2})

	cancelFunc()
	wg.Wait()

	expectedMetrics := []gostatsd.Metric{
		sm1(), sm1(),
	}

	expectedMetrics[0].Tags = gostatsd.Tags{"a1", "tag:value"}
	expectedMetrics[0].Hostname = "1.2.3.4"
	expectedMetrics[1].Tags = gostatsd.Tags{"a1", "tag:value"}
	expectedMetrics[1].Hostname = "1.2.3.4"

	assert.Equal(t, expectedMetrics, counting.metrics)
}

func TestCloudHandlerExpirationAndRefresh(t *testing.T) {
	// These still use a real clock, which means they're more susceptible to
	// CPU load triggering a race condition, therefore there's no t.Parallel()
	t.Run("4.3.2.1", func(t *testing.T) {
		testExpireAndRefresh(t, "4.3.2.1", func(h *CloudHandler) {
			e := se1()
			h.DispatchEvent(context.Background(), &e)
		})
	})
	t.Run("1.2.3.4", func(t *testing.T) {
		testExpireAndRefresh(t, "1.2.3.4", func(h *CloudHandler) {
			m := sm1()
			h.DispatchMetrics(context.Background(), []*gostatsd.Metric{&m})
		})
	})
}

func testExpireAndRefresh(t *testing.T, expectedIp gostatsd.IP, f func(*CloudHandler)) {
	fp := &fakeProviderIP{}
	counting := &countingHandler{}
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
	ch := NewCloudHandler(fp, counting, logrus.StandardLogger(), rate.NewLimiter(100, 120), CacheOptions{
		CacheRefreshPeriod:        11 * time.Millisecond,
		CacheEvictAfterIdlePeriod: 50 * time.Millisecond,
		CacheTTL:                  10 * time.Millisecond,
		CacheNegativeTTL:          100 * time.Millisecond,
	})
	var wg wait.Group
	defer wg.Wait()
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	wg.StartWithContext(ctx, ch.Run)
	f(ch)
	time.Sleep(70 * time.Millisecond) // Should be refreshed couple of times and evicted.

	cancelFunc()
	wg.Wait()

	// Cache might refresh multiple times, ensure it only refreshed with the expected IP
	for _, ip := range fp.ips {
		assert.Equal(t, expectedIp, ip)
	}
	assert.GreaterOrEqual(t, len(fp.ips), 2) // Ensure it does at least 1 lookup + 1 refresh
	assert.Zero(t, len(ch.cache))            // Ensure it eventually expired
}

func TestCloudHandlerDispatch(t *testing.T) {
	t.Parallel()
	fp := &fakeProviderIP{
		Tags: gostatsd.Tags{"region:us-west-3", "tag1", "tag2:234"},
	}

	expectedIps := []gostatsd.IP{"1.2.3.4", "4.3.2.1"}
	expectedMetrics := []gostatsd.Metric{
		{
			Name:     "t1",
			Value:    42.42,
			Tags:     gostatsd.Tags{"a1", "region:us-west-3", "tag1", "tag2:234"},
			Hostname: "i-1.2.3.4",
			SourceIP: "1.2.3.4",
			Type:     gostatsd.COUNTER,
		},
		{
			Name:     "t1",
			Value:    45.45,
			Tags:     gostatsd.Tags{"a4", "region:us-west-3", "tag1", "tag2:234"},
			Hostname: "i-1.2.3.4",
			SourceIP: "1.2.3.4",
			Type:     gostatsd.COUNTER,
		},
	}
	expectedEvents := gostatsd.Events{
		gostatsd.Event{
			Title:    "t12",
			Text:     "asrasdfasdr",
			Tags:     gostatsd.Tags{"a2", "region:us-west-3", "tag1", "tag2:234"},
			Hostname: "i-4.3.2.1",
			SourceIP: "4.3.2.1",
		},
		gostatsd.Event{
			Title:    "t1asdas",
			Text:     "asdr",
			Tags:     gostatsd.Tags{"a2-35", "region:us-west-3", "tag1", "tag2:234"},
			Hostname: "i-4.3.2.1",
			SourceIP: "4.3.2.1",
		},
	}
	doCheck(t, fp, sm1(), se1(), sm2(), se2(), &fp.ips, expectedIps, expectedMetrics, expectedEvents)
}

func TestCloudHandlerInstanceNotFound(t *testing.T) {
	t.Parallel()
	fp := &fakeProviderNotFound{}
	expectedIps := []gostatsd.IP{"1.2.3.4", "4.3.2.1"}
	expectedMetrics := []gostatsd.Metric{
		sm1(),
		sm2(),
	}
	expectedEvents := gostatsd.Events{
		se1(),
		se2(),
	}
	doCheck(t, fp, sm1(), se1(), sm2(), se2(), &fp.ips, expectedIps, expectedMetrics, expectedEvents)
}

func TestCloudHandlerFailingProvider(t *testing.T) {
	t.Parallel()
	fp := &fakeFailingProvider{}
	expectedIps := []gostatsd.IP{"1.2.3.4", "4.3.2.1"}
	expectedMetrics := []gostatsd.Metric{
		sm1(),
		sm2(),
	}
	expectedEvents := gostatsd.Events{
		se1(),
		se2(),
	}
	doCheck(t, fp, sm1(), se1(), sm2(), se2(), &fp.ips, expectedIps, expectedMetrics, expectedEvents)
}

func TestConstructCloudHandlerFactoryFromViper(t *testing.T) {
	t.Parallel()
	v := viper.New()
	logger := logrus.StandardLogger()

	// Test no cloud handler
	factory, err := ConstructCloudHandlerFactoryFromViper(v, logger, "test")
	assert.NoError(t, err)
	assert.Nil(t, factory.cloudProvider)

	// Test unknown cloud handler - unsupported
	v.Set(ParamCloudProvider, "unknown")
	_, err = ConstructCloudHandlerFactoryFromViper(v, logger, "test")
	assert.Error(t, err)

	// Test known cloud provider defaults
	cloudProvidersToTest := []string{aws.ProviderName, k8s.ProviderName}
	for _, cpName := range cloudProvidersToTest {
		v.Set(ParamCloudProvider, cpName)
		factory, err = ConstructCloudHandlerFactoryFromViper(v, logger, "test")
		assert.NoError(t, err)
		assert.EqualValues(t, DefaultCloudProviderCacheValues[cpName], factory.cacheOptions)
		assert.Equal(t, rate.Limit(DefaultCloudProviderLimiterValues[cpName].MaxCloudRequests), factory.limiter.Limit())
		assert.Equal(t, DefaultCloudProviderLimiterValues[cpName].BurstCloudRequests, factory.limiter.Burst())
	}
}

func TestInitCloudHandlerFactory(t *testing.T) {
	t.Parallel()
	v := viper.New()
	logger := logrus.StandardLogger()

	factory, err := ConstructCloudHandlerFactoryFromViper(v, logger, "test")
	assert.NoError(t, err)
	assert.Nil(t, factory.cloudProvider)

	err = factory.InitCloudProvider(v)
	assert.NoError(t, err)

	// We don't test the path for specific cloud providers as they have logic that isn't easily mockable
}

func doCheck(t *testing.T, cloud gostatsd.CloudProvider, m1 gostatsd.Metric, e1 gostatsd.Event, m2 gostatsd.Metric, e2 gostatsd.Event, ips *[]gostatsd.IP, expectedIps []gostatsd.IP, expectedM []gostatsd.Metric, expectedE gostatsd.Events) {
	counting := &countingHandler{}
	ch := NewCloudHandler(cloud, counting, logrus.StandardLogger(), rate.NewLimiter(100, 120), CacheOptions{
		CacheRefreshPeriod:        DefaultCacheRefreshPeriod,
		CacheEvictAfterIdlePeriod: DefaultCacheEvictAfterIdlePeriod,
		CacheTTL:                  DefaultCacheTTL,
		CacheNegativeTTL:          DefaultCacheNegativeTTL,
	})
	var wg wait.Group
	defer wg.Wait()
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	wg.StartWithContext(ctx, ch.Run)
	ch.DispatchMetrics(ctx, []*gostatsd.Metric{&m1})
	ch.DispatchEvent(ctx, &e1)
	time.Sleep(1 * time.Second)
	ch.DispatchMetrics(ctx, []*gostatsd.Metric{&m2})
	ch.DispatchEvent(ctx, &e2)
	time.Sleep(1 * time.Second)
	cancelFunc()
	wg.Wait()

	sort.Slice(*ips, func(i, j int) bool {
		return (*ips)[i] < (*ips)[j]
	})
	assert.Equal(t, expectedIps, *ips)
	assert.Equal(t, expectedM, counting.metrics)
	assert.Equal(t, expectedE, counting.events)
	assert.EqualValues(t, 1, cloud.(CountingProvider).Invocations())
}

func sm1() gostatsd.Metric {
	return gostatsd.Metric{
		Name:     "t1",
		Value:    42.42,
		Tags:     gostatsd.Tags{"a1"},
		Hostname: "somehost",
		SourceIP: "1.2.3.4",
		Type:     gostatsd.COUNTER,
	}
}

func sm2() gostatsd.Metric {
	return gostatsd.Metric{
		Name:     "t1",
		Value:    45.45,
		Tags:     gostatsd.Tags{"a4"},
		Hostname: "somehost",
		SourceIP: "1.2.3.4",
		Type:     gostatsd.COUNTER,
	}
}

func se1() gostatsd.Event {
	return gostatsd.Event{
		Title:    "t12",
		Text:     "asrasdfasdr",
		Tags:     gostatsd.Tags{"a2"},
		Hostname: "some_random_host",
		SourceIP: "4.3.2.1",
	}
}

func se2() gostatsd.Event {
	return gostatsd.Event{
		Title:    "t1asdas",
		Text:     "asdr",
		Tags:     gostatsd.Tags{"a2-35"},
		Hostname: "some_random_host",
		SourceIP: "4.3.2.1",
	}
}

type CountingProvider interface {
	Invocations() uint64
}

type fakeCountingProvider struct {
	mu          sync.Mutex
	ips         []gostatsd.IP
	invocations uint64
}

func (fp *fakeCountingProvider) EstimatedTags() int {
	return 0
}

func (fp *fakeCountingProvider) MaxInstancesBatch() int {
	return 16
}

func (fp *fakeCountingProvider) Invocations() uint64 {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return fp.invocations
}

func (fp *fakeCountingProvider) count(ips ...gostatsd.IP) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.ips = append(fp.ips, ips...)
	fp.invocations++
}

func (fp *fakeCountingProvider) SelfIP() (gostatsd.IP, error) {
	return gostatsd.UnknownIP, nil
}

type fakeProviderIP struct {
	fakeCountingProvider
	Region string
	Tags   gostatsd.Tags
}

func (fp *fakeProviderIP) Name() string {
	return "fakeProviderIP"
}

func (fp *fakeProviderIP) Instance(ctx context.Context, ips ...gostatsd.IP) (map[gostatsd.IP]*gostatsd.Instance, error) {
	fp.count(ips...)
	instances := make(map[gostatsd.IP]*gostatsd.Instance, len(ips))
	for _, ip := range ips {
		instances[ip] = &gostatsd.Instance{
			ID:   "i-" + string(ip),
			Tags: fp.Tags,
		}
	}
	return instances, nil
}

type fakeProviderNotFound struct {
	fakeCountingProvider
}

func (fp *fakeProviderNotFound) Name() string {
	return "fakeProviderNotFound"
}

func (fp *fakeProviderNotFound) Instance(ctx context.Context, ips ...gostatsd.IP) (map[gostatsd.IP]*gostatsd.Instance, error) {
	fp.count(ips...)
	return nil, nil
}

type fakeFailingProvider struct {
	fakeCountingProvider
}

func (fp *fakeFailingProvider) Name() string {
	return "fakeFailingProvider"
}

func (fp *fakeFailingProvider) Instance(ctx context.Context, ips ...gostatsd.IP) (map[gostatsd.IP]*gostatsd.Instance, error) {
	fp.count(ips...)
	return nil, errors.New("clear skies, no clouds available")
}

type fakeProviderTransient struct {
	call        uint64
	failureMode []int
}

func (fpt *fakeProviderTransient) Name() string {
	return "fakeProviderTransient"
}

func (fpt *fakeProviderTransient) EstimatedTags() int {
	return 1
}

func (fpt *fakeProviderTransient) MaxInstancesBatch() int {
	return 1
}

func (fpt *fakeProviderTransient) SelfIP() (gostatsd.IP, error) {
	return gostatsd.UnknownIP, nil
}

// Instance emulates a lookup based on the supplied criteria.
// A failure mode of 0 is a successful lookup
// A failure mode of 1 is nil instance, no error (lookup failure)
// A failure mode of 2 is nil instance, with error
// Repeats the last specified failure mode
func (fpt *fakeProviderTransient) Instance(ctx context.Context, ips ...gostatsd.IP) (map[gostatsd.IP]*gostatsd.Instance, error) {
	r := make(map[gostatsd.IP]*gostatsd.Instance)

	c := atomic.AddUint64(&fpt.call, 1) - 1
	if c >= uint64(len(fpt.failureMode)) {
		c = uint64(len(fpt.failureMode) - 1)
	}
	switch fpt.failureMode[c] {
	case 0:
		for _, ip := range ips {
			r[ip] = &gostatsd.Instance{
				ID:   string(ip),
				Tags: gostatsd.Tags{"tag:value"},
			}
		}
		return r, nil
	case 1:
		for _, ip := range ips {
			r[ip] = nil
		}
		return r, nil
	case 2:
		for _, ip := range ips {
			r[ip] = nil
		}
		return r, errors.New("failure mode 2")
	}
	return nil, nil
}
