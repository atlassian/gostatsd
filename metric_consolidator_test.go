package gostatsd

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tilinna/clock"
)

func TestConsolidation(t *testing.T) {
	t.Parallel()
	ctxTest, testDone := testContext(t)
	defer testDone()

	mockClock := clock.NewMock(time.Unix(0, 0))
	ctxClock := clock.Context(ctxTest, mockClock)

	ch := make(chan []*MetricMap, 1)
	mc := NewMetricConsolidator(2, 1*time.Second, ch)

	m1 := &Metric{
		Name:      "foo",
		Type:      COUNTER,
		Value:     1,
		Rate:      1,
		Timestamp: 10,
	}
	m2 := &Metric{
		Name:      "foo",
		Type:      COUNTER,
		Value:     3,
		Rate:      0.1,
		Timestamp: 20,
	}
	mc.ReceiveMetrics([]*Metric{m1})
	mc.ReceiveMetrics([]*Metric{m2})
	mc.Flush(ctxClock)

	mm := <-ch

	expected := []*MetricMap{NewMetricMap(), NewMetricMap()}
	expected[0].Counters["foo"] = map[string]Counter{
		"": {
			PerSecond: 0,
			Value:     1,
			Timestamp: 10,
			Hostname:  "",
			Tags:      nil,
		},
	}
	expected[1].Counters["foo"] = map[string]Counter{
		"": {
			PerSecond: 0,
			Value:     30,
			Timestamp: 20,
			Hostname:  "",
			Tags:      nil,
		},
	}

	require.EqualValues(t, expected, mm)
}

func randomMetric(seed, variations int) *Metric {
	m := &Metric{}
	m.Type = MetricType(1 + (seed % 4))
	seed /= 4
	m.Name = fmt.Sprintf("%d", seed%variations)
	seed /= variations
	m.Tags = Tags{fmt.Sprintf("key:%d", seed%variations)}
	if m.Type == SET {
		m.StringValue = fmt.Sprintf("%d", seed)
	} else {
		m.Value = float64(seed)
		m.Rate = 1
	}
	m.Timestamp = 10
	return m
}

func benchmarkMetricConsolidator(b *testing.B, parallelism, variations int) {
	var wgWork sync.WaitGroup

	var wgInfra sync.WaitGroup

	ch := make(chan []*MetricMap)
	mc := NewMetricConsolidator(3, 100*time.Millisecond, ch)

	ctx, cancel := context.WithCancel(context.Background())

	wgInfra.Add(2)
	go func() {
		// Keep the channel drained, contents don't matter
		for range ch {
		}
		wgInfra.Done()
	}()
	go func() {
		// Let it flush on its own interval
		mc.Run(ctx)
		close(ch)
		wgInfra.Done()
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < parallelism; i++ {
		wgWork.Add(1)
		go func() {
			for j := 0; j < b.N/parallelism; j++ {
				mc.ReceiveMetrics([]*Metric{randomMetric(j, variations)})
			}
			wgWork.Done()
		}()
	}
	wgWork.Wait()
	b.StopTimer()

	cancel()
	wgInfra.Wait()
}

func BenchmarkMetricConsolidator(b *testing.B) {
	for i := 1; i <= 8; i += i {
		for j := 1; j <= 4096; j *= 8 {
			b.Run(fmt.Sprintf("%d-routines-%d-variations", i, j), func(b *testing.B) {
				benchmarkMetricConsolidator(b, i, j)
			})
		}
	}
}
