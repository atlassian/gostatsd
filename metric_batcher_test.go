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

func TestMetricBatcherCount(t *testing.T) {
	// Ensure flush occurs when enough metrics are sent.
	t.Parallel()
	ctxTest, testDone := testContext(t)
	defer testDone()

	ch := make(chan []*Metric, 3)

	var metrics []*Metric

	mc := NewMetricBatcher(10, 100*time.Millisecond, ch)
	for i := 0; i < 25; i++ {
		mc.DispatchMetric(ctxTest, &Metric{})
	}
	close(ch)
	for ms := range ch {
		metrics = append(metrics, ms...)
	}
	require.EqualValues(t, 20, len(metrics))
}

func TestMetricBatcherInterval(t *testing.T) {
	// Ensure flush occurs on an interval.

	t.Parallel()
	ctxTest, testDone := testContext(t)
	defer testDone()

	mockClock := clock.NewMock(time.Unix(0, 0))
	ctxClock := clock.Context(ctxTest, mockClock)

	ch := make(chan []*Metric, 3)

	// metricCount is higher than the number of metrics we'll send.
	mc := NewMetricBatcher(100, 100*time.Millisecond, ch)

	go mc.Run(ctxClock)

	for i := 0; i < 25; i++ {
		mc.DispatchMetric(ctxTest, &Metric{})
	}

	// There's no good way to tell when the Ticker has been created, so we use a hard loop
	for _, d := mockClock.AddNext(); d == 0 && ctxTest.Err() == nil; _, d = mockClock.AddNext() {
		time.Sleep(time.Millisecond) // Allows the system to actually idle, runtime.Gosched() does not.
	}

	select {
	case <-ctxTest.Done():
	case metrics := <-ch:
		require.EqualValues(t, 25, len(metrics))
	}
}

func BenchmarkMetricBatcher(b *testing.B) {
	for i := 1; i <= 8; i += i {
		for j := 1; j <= 4096; j *= 8 {
			b.Run(fmt.Sprintf("%d-routines-%d-buffer", i, j), func(b *testing.B) {
				benchmarkMetricBatcher(b, i, j)
			})
		}
	}
}

func benchmarkMetricBatcher(b *testing.B, parallelism, bufferSize int) {
	ch := make(chan []*Metric)
	mc := NewMetricBatcher(bufferSize, 1*time.Second, ch)
	ctx, cancel := context.WithCancel(context.Background())

	var wgInfra sync.WaitGroup

	wgInfra.Add(1)
	go func() {
		for range ch {
		}
		wgInfra.Done()
	}()

	wgInfra.Add(1)
	go func() {
		mc.Run(ctx)
		wgInfra.Done()
	}()

	b.ResetTimer()
	var wgWork sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		wgWork.Add(1)
		go func() {
			for j := 0; j < b.N/parallelism; j++ {
				mc.DispatchMetric(ctx, &Metric{})
			}
			wgWork.Done()
		}()
	}
	wgWork.Wait()
	b.StopTimer()

	cancel()
	close(ch)
	wgInfra.Wait()
}
