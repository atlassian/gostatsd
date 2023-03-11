package statsd

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/ash2k/stager/wait"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tilinna/clock"
	"golang.org/x/time/rate"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/fixtures"
	"github.com/atlassian/gostatsd/pkg/cachedinstances/cloudprovider"
	"github.com/atlassian/gostatsd/pkg/cloudproviders/fakeprovider"
)

// BenchmarkCloudHandlerDispatchMetricMap is a benchmark intended to (manually) test
// the impact of the CloudHandler.statsCacheHit field.
func BenchmarkCloudHandlerDispatchMetricMap(b *testing.B) {
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
		mm := gostatsd.NewMetricMap(false)
		mm.Receive(sm1())

		for pb.Next() {
			ch.DispatchMetricMap(ctxBackground, mm)
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

	expecting := &expectingHandler{}

	ci := cloudprovider.NewCachedCloudProvider(logrus.StandardLogger(), rate.NewLimiter(100, 120), fpt, gostatsd.CacheOptions{
		CacheRefreshPeriod:        50 * time.Millisecond,
		CacheEvictAfterIdlePeriod: 1 * time.Minute,
		CacheTTL:                  1 * time.Millisecond,
		CacheNegativeTTL:          1 * time.Millisecond,
	})
	ch := NewCloudHandler(ci, expecting)

	// t+0: instance is queried, goes in cache
	// t+50ms: instance refreshed (failure)
	// t+100ms: instance is queried, must be in cache

	var wg wait.Group
	defer wg.Wait()
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	clck := clock.NewMock(time.Unix(0, 0))
	ctx = clock.Context(ctx, clck)
	wg.StartWithContext(ctx, ch.Run)
	wg.StartWithContext(ctx, ci.Run)
	m1 := sm1()
	m2 := sm2()

	// There's no good way to tell when the Ticker has been created, so we use a hard loop
	for _, d := clck.AddNext(); d == 0 && ctx.Err() == nil; _, d = clck.AddNext() {
		time.Sleep(time.Millisecond) // Allows the system to actually idle, runtime.Gosched() does not.
	}

	// t+0: prime the cache
	expecting.Expect(1, 0)
	mm := gostatsd.NewMetricMap(false)
	mm.Receive(m1)
	ch.DispatchMetricMap(ctx, mm)
	expecting.WaitAll()

	clck.Add(50 * time.Millisecond)
	// t+50: refresh
	clck.Add(50 * time.Millisecond)

	// t+100ms: read from cache, must still be valid
	expecting.Expect(1, 0)

	mm = gostatsd.NewMetricMap(false)
	mm.Receive(m2)
	ch.DispatchMetricMap(ctx, mm)
	expecting.WaitAll()

	cancelFunc()
	wg.Wait()

	expectedMetrics := []*gostatsd.Metric{sm1(), sm2()}

	expectedMetrics[0].Tags = gostatsd.Tags{"a1", "tag:value"}
	expectedMetrics[0].Source = "1.2.3.4"
	expectedMetrics[1].Tags = gostatsd.Tags{"a4", "tag:value"}
	expectedMetrics[1].Source = "1.2.3.4"

	actual := gostatsd.MergeMaps(expecting.MetricMaps()).AsMetrics()

	sort.Slice(expectedMetrics, fixtures.SortCompare(expectedMetrics))
	sort.Slice(actual, fixtures.SortCompare(actual))
	for _, em := range expectedMetrics {
		em.FormatTagsKey()
	}

	require.Equal(t, expectedMetrics, actual)
}

func TestCloudHandlerDispatch(t *testing.T) {
	t.Parallel()
	fp := &fakeprovider.IP{
		Tags: gostatsd.Tags{"region:us-west-3", "tag1", "tag2:234"},
	}

	expectedIps := []gostatsd.Source{"1.2.3.4", "4.3.2.1"}
	expectedMetrics := []*gostatsd.Metric{
		{
			Name:   "t1",
			Value:  42,
			Rate:   1,
			Tags:   gostatsd.Tags{"a1", "region:us-west-3", "tag1", "tag2:234"},
			Source: "i-1.2.3.4",
			Type:   gostatsd.COUNTER,
		},
		{
			Name:   "t1",
			Value:  45,
			Rate:   1,
			Tags:   gostatsd.Tags{"a4", "region:us-west-3", "tag1", "tag2:234"},
			Source: "i-1.2.3.4",
			Type:   gostatsd.COUNTER,
		},
	}
	expectedEvents := gostatsd.Events{
		{
			Title:  "t12",
			Text:   "asrasdfasdr",
			Tags:   gostatsd.Tags{"a2", "region:us-west-3", "tag1", "tag2:234"},
			Source: "i-4.3.2.1",
		},
		{
			Title:  "t1asdas",
			Text:   "asdr",
			Tags:   gostatsd.Tags{"a2-35", "region:us-west-3", "tag1", "tag2:234"},
			Source: "i-4.3.2.1",
		},
	}
	doCheck(t, fp, sm1(), se1(), sm2(), se2(), fp.IPs, expectedIps, expectedMetrics, expectedEvents)
}

func TestCloudHandlerInstanceNotFound(t *testing.T) {
	t.Parallel()
	fp := &fakeprovider.NotFound{}
	expectedIps := []gostatsd.Source{"1.2.3.4", "4.3.2.1"}
	expectedMetrics := []*gostatsd.Metric{
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
	expectedIps := []gostatsd.Source{"1.2.3.4", "4.3.2.1"}
	expectedMetrics := []*gostatsd.Metric{
		sm1(),
		sm2(),
	}
	expectedEvents := gostatsd.Events{
		se1(),
		se2(),
	}
	doCheck(t, fp, sm1(), se1(), sm2(), se2(), fp.IPs, expectedIps, expectedMetrics, expectedEvents)
}

func doCheck(
	t *testing.T,
	cloud CountingProvider,
	m1 *gostatsd.Metric,
	e1 *gostatsd.Event,
	m2 *gostatsd.Metric,
	e2 *gostatsd.Event,
	ipsFunc func() []gostatsd.Source,
	expectedIps []gostatsd.Source,
	expectedM []*gostatsd.Metric,
	expectedE gostatsd.Events,
) {
	expecting := &expectingHandler{}
	ci := cloudprovider.NewCachedCloudProvider(logrus.StandardLogger(), rate.NewLimiter(100, 120), cloud, gostatsd.CacheOptions{
		CacheRefreshPeriod:        gostatsd.DefaultCacheRefreshPeriod,
		CacheEvictAfterIdlePeriod: gostatsd.DefaultCacheEvictAfterIdlePeriod,
		CacheTTL:                  gostatsd.DefaultCacheTTL,
		CacheNegativeTTL:          gostatsd.DefaultCacheNegativeTTL,
	})
	ch := NewCloudHandler(ci, expecting)

	var wg wait.Group
	defer wg.Wait()
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	wg.StartWithContext(ctx, ch.Run)
	wg.StartWithContext(ctx, ci.Run)

	expecting.Expect(1, 1)
	mm := gostatsd.NewMetricMap(false)
	mm.Receive(m1)
	ch.DispatchMetricMap(ctx, mm)
	ch.DispatchEvent(ctx, e1)
	expecting.WaitAll()

	expecting.Expect(1, 1)
	mm = gostatsd.NewMetricMap(false)
	mm.Receive(m2)
	ch.DispatchMetricMap(ctx, mm)
	ch.DispatchEvent(ctx, e2)
	expecting.WaitAll()

	cancelFunc()
	wg.Wait()

	ips := ipsFunc()
	sort.Slice(ips, func(i, j int) bool {
		return ips[i] < ips[j]
	})
	assert.Equal(t, expectedIps, ips)
	actual := gostatsd.MergeMaps(expecting.MetricMaps()).AsMetrics()
	sort.Slice(expectedM, fixtures.SortCompare(expectedM))
	sort.Slice(actual, fixtures.SortCompare(actual))
	for _, em := range expectedM {
		em.FormatTagsKey()
	}
	assert.Equal(t, expectedM, actual)
	assert.Equal(t, expectedE, expecting.Events())
	assert.LessOrEqual(t, cloud.Invocations(), uint64(2))
}

func sm1() *gostatsd.Metric {
	return &gostatsd.Metric{
		Name:   "t1",
		Value:  42,
		Rate:   1,
		Tags:   gostatsd.Tags{"a1"},
		Source: "1.2.3.4",
		Type:   gostatsd.COUNTER,
	}
}

func sm2() *gostatsd.Metric {
	return &gostatsd.Metric{
		Name:   "t1",
		Value:  45,
		Rate:   1,
		Tags:   gostatsd.Tags{"a4"},
		Source: "1.2.3.4",
		Type:   gostatsd.COUNTER,
	}
}

func se1() *gostatsd.Event {
	return &gostatsd.Event{
		Title:  "t12",
		Text:   "asrasdfasdr",
		Tags:   gostatsd.Tags{"a2"},
		Source: "4.3.2.1",
	}
}

func se2() *gostatsd.Event {
	return &gostatsd.Event{
		Title:  "t1asdas",
		Text:   "asdr",
		Tags:   gostatsd.Tags{"a2-35"},
		Source: "4.3.2.1",
	}
}

type CountingProvider interface {
	gostatsd.CloudProvider
	Invocations() uint64
}
