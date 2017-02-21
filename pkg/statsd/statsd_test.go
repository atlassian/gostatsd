package statsd

import (
	"context"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/fakesocket"

	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

// TestStatsdThroughput emulates statsd work using fake network socket and null backend to
// measure throughput.
func TestStatsdThroughput(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	var memStatsStart, memStatsFinish runtime.MemStats
	runtime.ReadMemStats(&memStatsStart)
	backend := &countingBackend{}
	s := Server{
		Backends: []gostatsd.Backend{backend},
		CloudProvider: &fakeProvider{
			instance: &gostatsd.Instance{
				ID:     "i-13123123",
				Region: "us-west-3",
				Tags:   gostatsd.Tags{"tag1", "tag2:234"},
			},
		},
		Limiter:          rate.NewLimiter(DefaultMaxCloudRequests, DefaultBurstCloudRequests),
		DefaultTags:      DefaultTags,
		ExpiryInterval:   DefaultExpiryInterval,
		FlushInterval:    DefaultFlushInterval,
		MaxReaders:       DefaultMaxReaders,
		MaxWorkers:       DefaultMaxWorkers,
		MaxQueueSize:     DefaultMaxQueueSize,
		PercentThreshold: DefaultPercentThreshold,
		CacheOptions: CacheOptions{
			CacheRefreshPeriod:        DefaultCacheRefreshPeriod,
			CacheEvictAfterIdlePeriod: DefaultCacheEvictAfterIdlePeriod,
			CacheTTL:                  DefaultCacheTTL,
			CacheNegativeTTL:          DefaultCacheNegativeTTL,
		},
		Viper: viper.New(),
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	err := s.RunWithCustomSocket(ctx, fakesocket.Factory)
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		t.Errorf("statsd run failed: %v", err)
	}
	runtime.ReadMemStats(&memStatsFinish)
	totalAlloc := memStatsFinish.TotalAlloc - memStatsStart.TotalAlloc
	heapObjects := memStatsFinish.HeapObjects - memStatsStart.HeapObjects
	numMetrics := backend.metrics
	mallocs := memStatsFinish.Mallocs - memStatsStart.Mallocs
	t.Logf(`Processed metrics: %d
	TotalAlloc: %d (%d per metric)
	HeapObjects: %d
	Mallocs: %d (%d per metric)
	NumGC: %d
	GCCPUFraction: %f`,
		numMetrics,
		totalAlloc, totalAlloc/numMetrics,
		heapObjects,
		mallocs, mallocs/numMetrics,
		memStatsFinish.NumGC-memStatsStart.NumGC,
		memStatsFinish.GCCPUFraction)
}

type countingBackend struct {
	metrics uint64
	events  uint64
}

func (cb *countingBackend) Name() string {
	return "countingBackend"
}

func (cb *countingBackend) SendMetricsAsync(ctx context.Context, m *gostatsd.MetricMap, callback gostatsd.SendCallback) {
	atomic.AddUint64(&cb.metrics, uint64(m.NumStats))
	callback(nil)
}

func (cb *countingBackend) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	atomic.AddUint64(&cb.events, 1)
	return nil
}

type fakeProvider struct {
	instance *gostatsd.Instance
}

func (fp *fakeProvider) Name() string {
	return "fakeProvider"
}

func (p *fakeProvider) MaxInstancesBatch() int {
	return 16
}

func (fp *fakeProvider) Instance(ctx context.Context, ips ...gostatsd.IP) (map[gostatsd.IP]*gostatsd.Instance, error) {
	instances := make(map[gostatsd.IP]*gostatsd.Instance, len(ips))
	for _, ip := range ips {
		instances[ip] = fp.instance
	}
	return instances, nil
}

func (fp *fakeProvider) SelfIP() (gostatsd.IP, error) {
	return gostatsd.UnknownIP, nil
}
