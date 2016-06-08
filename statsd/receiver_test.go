package statsd

import (
	"runtime"
	"testing"

	"github.com/atlassian/gostatsd/tester/fakesocket"
	"github.com/atlassian/gostatsd/types"

	"golang.org/x/net/context"
)

var receiveBlackhole error

func TestReceiveEmptyPacket(t *testing.T) {
	input := [][]byte{
		{},
		{'\n'},
		{'\n', '\n'},
	}
	var memStatsStart, memStatsFinish runtime.MemStats
	runtime.ReadMemStats(&memStatsStart)
	for _, inp := range input {
		ch := &countingHandler{}
		mr := NewMetricReceiver("", []string{}, ch).(*metricReceiver)

		err := mr.handlePacket(context.Background(), fakesocket.FakeAddr{}, inp)
		if err != nil {
			t.Errorf("%q: unexpected error: %v", err)
		}
		if len(ch.events) > 0 {
			t.Errorf("%q: expected no events: %s", inp, ch.events)
		}
		if len(ch.metrics) > 0 {
			t.Errorf("%q: expected no metrics: %s", inp, ch.metrics)
		}
	}
	runtime.ReadMemStats(&memStatsFinish)
	totalAlloc := memStatsFinish.TotalAlloc - memStatsStart.TotalAlloc
	mallocs := memStatsFinish.Mallocs - memStatsStart.Mallocs
	t.Logf("TotalAlloc: %d\nMallocs: %d\nNumGC: %d\nGCCPUFraction: %f",
		totalAlloc,
		mallocs,
		memStatsFinish.NumGC-memStatsStart.NumGC,
		memStatsFinish.GCCPUFraction)
}

func BenchmarkReceive(b *testing.B) {
	mr := &metricReceiver{
		handler: nopHandler{},
	}
	c := fakesocket.FakePacketConn{}
	ctx := context.Background()
	var r error
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		r = mr.Receive(ctx, c)
	}
	receiveBlackhole = r
}

type nopHandler struct{}

func (h nopHandler) DispatchMetric(ctx context.Context, m *types.Metric) error {
	return context.Canceled // Stops receiver after first read is done
}

func (h nopHandler) DispatchEvent(ctx context.Context, e *types.Event) error {
	return context.Canceled // Stops receiver after first read is done
}
