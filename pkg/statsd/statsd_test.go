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
		Viper:            viper.New(),
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

func (cb *countingBackend) SampleConfig() string {
	return ""
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

func (fp *fakeProvider) SampleConfig() string {
	return ""
}

func (fp *fakeProvider) Instance(ctx context.Context, IP gostatsd.IP) (*gostatsd.Instance, error) {
	return fp.instance, nil
}

func (fp *fakeProvider) SelfIP() (gostatsd.IP, error) {
	return gostatsd.UnknownIP, nil
}
