package statsd

import (
	"context"
	"math/rand"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ash2k/stager/wait"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/cachedinstances/cloudprovider"
	"github.com/atlassian/gostatsd/pkg/fakesocket"
)

// TestStatsdStartStopEvents is a somewhat messy test
// which starts up the entire pipeline, and ensures that
// both start and stop events make it through to the backend.
func TestStatsdStartStopEvents(t *testing.T) {
	backend := &countingBackend{}
	cloudProvider := &fakeProvider{
		instance: &gostatsd.Instance{},
	}
	cachedInstances := cloudprovider.NewCachedCloudProvider(
		logrus.StandardLogger(),
		rate.NewLimiter(gostatsd.DefaultMaxCloudRequests, gostatsd.DefaultBurstCloudRequests),
		cloudProvider,
		gostatsd.CacheOptions{
			CacheRefreshPeriod: gostatsd.DefaultCacheRefreshPeriod,
		},
	)
	s := Server{
		Backends:            []gostatsd.Backend{backend},
		CachedInstances:     cachedInstances,
		DefaultTags:         gostatsd.DefaultTags,
		FlushInterval:       gostatsd.DefaultFlushInterval,
		MaxConcurrentEvents: 2,
		ServerMode:          "standalone",
		Viper:               viper.New(),
	}
	ctxCached, cancelCached := context.WithCancel(context.Background())
	defer cancelCached()
	var wg wait.Group
	wg.StartWithContext(ctxCached, cachedInstances.Run)
	ctxCancelled, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	err := s.RunWithCustomSocket(ctxCancelled, fakesocket.Factory)
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		t.Errorf("statsd run failed: %v", err)
	}
	cancelCached()
	wg.Wait()

	// Expecting 2 events, one for startup, one for shutdown.
	require.EqualValues(t, 2, backend.events)

}

// TestStatsdThroughput emulates statsd work using fake network socket and null backend to
// measure throughput.
func TestStatsdThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rand.Seed(time.Now().UnixNano())
	var memStatsStart, memStatsFinish runtime.MemStats
	runtime.ReadMemStats(&memStatsStart)
	backend := &countingBackend{}
	cloudProvider := &fakeProvider{
		instance: &gostatsd.Instance{
			ID:   "i-13123123",
			Tags: gostatsd.Tags{"region:us-west-3", "tag1", "tag2:234"},
		},
	}
	cachedInstances := cloudprovider.NewCachedCloudProvider(
		logrus.StandardLogger(),
		rate.NewLimiter(gostatsd.DefaultMaxCloudRequests, gostatsd.DefaultBurstCloudRequests),
		cloudProvider,
		gostatsd.CacheOptions{
			CacheRefreshPeriod:        gostatsd.DefaultCacheRefreshPeriod,
			CacheEvictAfterIdlePeriod: gostatsd.DefaultCacheEvictAfterIdlePeriod,
			CacheTTL:                  gostatsd.DefaultCacheTTL,
			CacheNegativeTTL:          gostatsd.DefaultCacheNegativeTTL,
		})
	s := Server{
		Backends:              []gostatsd.Backend{backend},
		CachedInstances:       cachedInstances,
		DefaultTags:           gostatsd.DefaultTags,
		ExpiryIntervalCounter: gostatsd.DefaultExpiryInterval,
		ExpiryIntervalSet:     gostatsd.DefaultExpiryInterval,
		ExpiryIntervalGauge:   gostatsd.DefaultExpiryInterval,
		ExpiryIntervalTimer:   gostatsd.DefaultExpiryInterval,
		FlushInterval:         gostatsd.DefaultFlushInterval,
		FlushOffset:           0,
		FlushAligned:          false,
		MaxReaders:            gostatsd.DefaultMaxReaders,
		MaxParsers:            gostatsd.DefaultMaxParsers,
		MaxWorkers:            gostatsd.DefaultMaxWorkers,
		MaxQueueSize:          gostatsd.DefaultMaxQueueSize,
		EstimatedTags:         1, // Travis has limited memory
		PercentThreshold:      gostatsd.DefaultPercentThreshold,
		HeartbeatEnabled:      gostatsd.DefaultHeartbeatEnabled,
		ReceiveBatchSize:      gostatsd.DefaultReceiveBatchSize,
		MaxConcurrentEvents:   2,
		ServerMode:            "standalone",
		Viper:                 viper.New(),
	}
	ctxCached, cancelCached := context.WithCancel(context.Background())
	defer cancelCached()
	var wg wait.Group
	wg.StartWithContext(ctxCached, cachedInstances.Run)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	start := time.Now()
	err := s.RunWithCustomSocket(ctx, fakesocket.Factory)
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		t.Errorf("statsd run failed: %v", err)
	}
	cancelCached()
	wg.Wait()
	duration := float64(time.Since(start)) / float64(time.Second)

	runtime.ReadMemStats(&memStatsFinish)
	totalAlloc := memStatsFinish.TotalAlloc - memStatsStart.TotalAlloc
	heapObjects := memStatsFinish.HeapObjects - memStatsStart.HeapObjects
	numMetrics := backend.metrics
	mallocs := memStatsFinish.Mallocs - memStatsStart.Mallocs
	t.Logf(`Processed metrics: %d (%f per second)
	TotalAlloc: %d (%d per metric)
	HeapObjects: %d
	Mallocs: %d (%d per metric)
	NumGC: %d
	GCCPUFraction: %f`,
		numMetrics, float64(numMetrics)/duration,
		totalAlloc, totalAlloc/numMetrics,
		heapObjects,
		mallocs, mallocs/numMetrics,
		memStatsFinish.NumGC-memStatsStart.NumGC,
		memStatsFinish.GCCPUFraction)
}

func TestNetworkFromAddress(t *testing.T) {
	t.Parallel()
	input := []struct {
		address         string
		expectedNetwork string
	}{
		{
			address:         "localhost:8125",
			expectedNetwork: "udp",
		},
		{
			address:         "",
			expectedNetwork: "udp",
		},
		{
			address:         "/some/file.sock",
			expectedNetwork: "unixgram",
		},
	}
	for pos, inp := range input {
		inp := inp
		t.Run(strconv.Itoa(pos), func(t *testing.T) {
			t.Parallel()
			network := networkFromAddress(inp.address)
			assert.Equal(t, inp.expectedNetwork, network)
		})
	}
}

type countingBackend struct {
	metrics uint64
	events  uint64
}

func (cb *countingBackend) Name() string {
	return "countingBackend"
}

func (cb *countingBackend) SendMetricsAsync(ctx context.Context, m *gostatsd.MetricMap, callback gostatsd.SendCallback) {
	count := 0
	m.Counters.Each(func(name, tagset string, c gostatsd.Counter) {
		count++
	})
	m.Gauges.Each(func(name, tagset string, g gostatsd.Gauge) {
		count++
	})
	m.Timers.Each(func(name, tagset string, t gostatsd.Timer) {
		count++
	})
	m.Sets.Each(func(name, tagset string, s gostatsd.Set) {
		count++
	})
	atomic.AddUint64(&cb.metrics, uint64(count))
	callback(nil)
}

func (cb *countingBackend) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	atomic.AddUint64(&cb.events, 1)
	return nil
}

type fakeProvider struct {
	instance *gostatsd.Instance
}

func (fp *fakeProvider) EstimatedTags() int {
	return len(fp.instance.Tags)
}

func (fp *fakeProvider) Name() string {
	return "fakeProvider"
}

func (fp *fakeProvider) MaxInstancesBatch() int {
	return 16
}

func (fp *fakeProvider) Instance(ctx context.Context, ips ...gostatsd.Source) (map[gostatsd.Source]*gostatsd.Instance, error) {
	instances := make(map[gostatsd.Source]*gostatsd.Instance, len(ips))
	for _, ip := range ips {
		instances[ip] = fp.instance
	}
	return instances, nil
}
