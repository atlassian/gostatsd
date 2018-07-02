package datadogexp

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	//"github.com/stretchr/testify/assert"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/tilinna/clock"
)

func makeTestBatcher(ctx context.Context, interval time.Duration, batchSize, reqBufSize int) (
	*metricBatcher,
	<-chan struct{},
	chan<- ddMetricMap,
	<-chan ddTimeSeries,
) {
	reqData := make(chan struct{}, reqBufSize)
	rcvData := make(chan ddMetricMap)
	subBatch := make(chan ddTimeSeries)

	mb := &metricBatcher{
		logger:          logrus.StandardLogger(),
		interval:        interval,
		metricsPerBatch: batchSize,
		requestData:     reqData,
		receiveData:     rcvData,
		submitBatch:     subBatch,
	}

	go mb.Run(ctx)
	return mb, reqData, rcvData, subBatch

}

func TestMetricBatcherRequestsDataMultipleTimes(t *testing.T) {
	t.Parallel()

	c := clock.NewMock(time.Unix(0, 0))
	realCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	ctx := clock.Context(realCtx, c)
	defer cancel()

	// It's possible the test isn't ready to receive from reqData by the time
	// the metricBatcher is trying to signal, so make the channel buffered.
	_, reqData, _, _ := makeTestBatcher(ctx, 10*time.Second, 1, 1)

	// Put current time halfway between intervals to avoid dealing with < vs <=
	c.Add(5 * time.Second)

	for i := 0; i < 3; i++ {
		select {
		case <-realCtx.Done():
			require.Fail(t, "timeout waiting for pull")
		case <-reqData:
		}
		c.Add(10 * time.Second)
	}
}

func m1() *ddMetric {
	return &ddMetric{
		Host:     "hostname",
		Interval: 10,
		Metric:   "name",
		Points: []ddPoint{
			{0, 0},
		},
		Tags: nil,
		Type: "gauge",
	}
}

func m2() *ddMetric {
	return &ddMetric{
		Host:     "hostname",
		Interval: 10,
		Metric:   "name",
		Points: []ddPoint{
			{0, 0},
		},
		Tags: []string{"key:value"},
		Type: "gauge",
	}
}

func m3() *ddMetric {
	return &ddMetric{
		Host:     "hostname",
		Interval: 10,
		Metric:   "name.rate",
		Points: []ddPoint{
			{0, 0},
		},
		Tags: []string{"key:value"},
		Type: "rate",
	}
}

func TestMetricBatcherSplitsBatches(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, _, rcvData, subBatch := makeTestBatcher(ctx, 10*time.Second, 1, 0)

	mm := ddMetricMap{
		"name": {
			"s.hostname":           m1(),
			"key.value.s.hostname": m2(),
		},
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout pushing metric map")
	case rcvData <- mm:
	}

	// order is non-determinstic, check if the result is in the array
	expected := []ddTimeSeries{
		{Series: []*ddMetric{m1()}},
		{Series: []*ddMetric{m2()}},
	}
	var ts1, ts2 ddTimeSeries

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout pulling batch 1")
	case ts1 = <-subBatch:
		require.Contains(t, expected, ts1, "not equal 1")
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout pulling batch 2")
	case ts2 = <-subBatch:
		require.Contains(t, expected, ts2, "not equal 2")
	}

	// make sure we didn't get the same result twice
	require.NotEqual(t, ts1, ts2, "received same ts twice")
}

func TestMetricBatcherSendsFinalBatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	mb, _, rcvData, subBatch := makeTestBatcher(ctx, 10*time.Second, 2, 0)
	mb.metricsPerBatch = 2

	mm := ddMetricMap{
		"name": {
			"s.hostname":           m1(),
			"key.value.s.hostname": m2(),
		},
		"name.rate": {
			"key.value.s.hostname": m3(),
		},
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout pushing metric map")
	case rcvData <- mm:
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout pulling batch 1")
	case ts := <-subBatch:
		require.Equal(t, 2, len(ts.Series), "expecting 2 metrics")
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout pulling batch 2")
	case ts := <-subBatch:
		require.Equal(t, 1, len(ts.Series), "expecting 1 metric")
	}
}

func benchmarkMetricBatcher(b *testing.B, batchSize, nameLimit int) {
	_, _, rcvData, subBatch := makeTestBatcher(context.Background(), 1*time.Second, batchSize, 0)

	mm := ddMetricMap{}
	rnd := rand.New(rand.NewSource(0))
	for i := 0; i < b.N/batchSize; i++ {
		name := strconv.Itoa(int(rnd.Int31n(int32(nameLimit))))
		c := strconv.Itoa(len(mm[name]))
		if mm[name] == nil {
			mm[name] = map[string]*ddMetric{}
		}
		mm[name]["tag.value.s."+c] = &ddMetric{
			Host:     c,
			Interval: 10,
			Metric:   name,
			Points: []ddPoint{
				{0, 0},
			},
			Tags: []string{"tag:value"},
			Type: "rate",
		}
	}

	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < batchSize; i++ {
		rcvData <- mm
		rcvd := 0
		for rcvd < b.N/batchSize {
			ts := <-subBatch
			rcvd += len(ts.Series)
		}
	}
}

func BenchmarkMetricBatch(b *testing.B) {
	for nameLimit := 1000; nameLimit <= 100000; nameLimit *= 10 {
		for batchSize := 499; batchSize < 1000; batchSize += 500 { // non-factor of b.N
			b.Run(fmt.Sprintf("n%d-b%d", nameLimit, batchSize), func(b *testing.B) {
				benchmarkMetricBatcher(b, batchSize, nameLimit)
			})
		}
	}
}
