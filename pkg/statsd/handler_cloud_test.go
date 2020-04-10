package statsd

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/ash2k/stager/wait"
	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/cachedinstances/cloudprovider"
	"github.com/atlassian/gostatsd/pkg/cloudproviders/fakeprovider"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

// BenchmarkCloudHandlerDispatchMetric is a benchmark intended to (manually) test
// the impact of the CloudHandler.statsCacheHit field.
func BenchmarkCloudHandlerDispatchMetric(b *testing.B) {
	fp := &fakeprovider.IP{}
	nh := &nopHandler{}
	ci := cloudprovider.NewCachedCloudProvider(logrus.StandardLogger(), rate.NewLimiter(100, 120), fp, gostatsd.CacheOptions{
		CacheRefreshPeriod:        100 * time.Millisecond,
		CacheEvictAfterIdlePeriod: 700 * time.Millisecond,
		CacheTTL:                  500 * time.Millisecond,
		CacheNegativeTTL:          500 * time.Millisecond,
	})
	ch := NewCloudHandler(ci, nh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ci.Run(ctx)
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
	fpt := &fakeprovider.Transient{
		FailureMode: []int{
			0, // success, prime the cache
			1, // soft failure, data should remain in cache
		},
	}

	counting := &countingHandler{}

	ci := cloudprovider.NewCachedCloudProvider(logrus.StandardLogger(), rate.NewLimiter(100, 120), fpt, gostatsd.CacheOptions{
		CacheRefreshPeriod:        50 * time.Millisecond,
		CacheEvictAfterIdlePeriod: 1 * time.Minute,
		CacheTTL:                  1 * time.Millisecond,
		CacheNegativeTTL:          1 * time.Millisecond,
	})
	ch := NewCloudHandler(ci, counting)

	// t+0: instance is queried, goes in cache
	// t+50ms: instance refreshed (failure)
	// t+100ms: instance is queried, must be in cache

	var wg wait.Group
	defer wg.Wait()
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	wg.StartWithContext(ctx, ch.Run)
	wg.StartWithContext(ctx, ci.Run)
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

	assert.Equal(t, expectedMetrics, counting.Metrics())
}

func TestCloudHandlerDispatch(t *testing.T) {
	t.Parallel()
	fp := &fakeprovider.IP{
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
	doCheck(t, fp, sm1(), se1(), sm2(), se2(), fp.IPs, expectedIps, expectedMetrics, expectedEvents)
}

func TestCloudHandlerInstanceNotFound(t *testing.T) {
	t.Parallel()
	fp := &fakeprovider.NotFound{}
	expectedIps := []gostatsd.IP{"1.2.3.4", "4.3.2.1"}
	expectedMetrics := []gostatsd.Metric{
		sm1(),
		sm2(),
	}
	expectedEvents := gostatsd.Events{
		se1(),
		se2(),
	}
	doCheck(t, fp, sm1(), se1(), sm2(), se2(), fp.IPs, expectedIps, expectedMetrics, expectedEvents)
}

func TestCloudHandlerFailingProvider(t *testing.T) {
	t.Parallel()
	fp := &fakeprovider.Failing{}
	expectedIps := []gostatsd.IP{"1.2.3.4", "4.3.2.1"}
	expectedMetrics := []gostatsd.Metric{
		sm1(),
		sm2(),
	}
	expectedEvents := gostatsd.Events{
		se1(),
		se2(),
	}
	doCheck(t, fp, sm1(), se1(), sm2(), se2(), fp.IPs, expectedIps, expectedMetrics, expectedEvents)
}

func doCheck(t *testing.T, cloud CountingProvider, m1 gostatsd.Metric, e1 gostatsd.Event, m2 gostatsd.Metric, e2 gostatsd.Event, ipsFunc func() []gostatsd.IP, expectedIps []gostatsd.IP, expectedM []gostatsd.Metric, expectedE gostatsd.Events) {
	counting := &countingHandler{}
	ci := cloudprovider.NewCachedCloudProvider(logrus.StandardLogger(), rate.NewLimiter(100, 120), cloud, gostatsd.CacheOptions{
		CacheRefreshPeriod:        gostatsd.DefaultCacheRefreshPeriod,
		CacheEvictAfterIdlePeriod: gostatsd.DefaultCacheEvictAfterIdlePeriod,
		CacheTTL:                  gostatsd.DefaultCacheTTL,
		CacheNegativeTTL:          gostatsd.DefaultCacheNegativeTTL,
	})
	ch := NewCloudHandler(ci, counting)

	var wg wait.Group
	defer wg.Wait()
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	wg.StartWithContext(ctx, ch.Run)
	wg.StartWithContext(ctx, ci.Run)
	ch.DispatchMetrics(ctx, []*gostatsd.Metric{&m1})
	ch.DispatchEvent(ctx, &e1)
	time.Sleep(1 * time.Second)
	ch.DispatchMetrics(ctx, []*gostatsd.Metric{&m2})
	ch.DispatchEvent(ctx, &e2)
	time.Sleep(10 * time.Second)
	cancelFunc()
	wg.Wait()

	ips := ipsFunc()
	sort.Slice(ips, func(i, j int) bool {
		return ips[i] < ips[j]
	})
	assert.Equal(t, expectedIps, ips)
	assert.Equal(t, expectedM, counting.Metrics())
	assert.Equal(t, expectedE, counting.Events())
	assert.EqualValues(t, 1, cloud.Invocations())
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
	gostatsd.CloudProvider
	Invocations() uint64
}
