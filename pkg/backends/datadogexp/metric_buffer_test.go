package datadogexp

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/atlassian/gostatsd"
)

func makeTestMetricBuffer(ctx context.Context, interval time.Duration, bufferSize int, disabled *gostatsd.TimerSubtypes) (
	chan<- *batchMessage,
	chan<- struct{},
	<-chan ddMetricMap,
) {
	metricsQueue := make(chan *batchMessage, bufferSize)
	bufferRequested := make(chan struct{})
	bufferReady := make(chan ddMetricMap)

	mb := &metricBuffer{
		logger:           logrus.StandardLogger(),
		disabledSubTypes: disabled,
		flushIntervalSec: interval.Seconds(),
		capacity:         10,
		metricsQueue:     metricsQueue,
		bufferRequested:  bufferRequested,
		bufferReady:      bufferReady,
	}

	go mb.Run(ctx)
	return metricsQueue, bufferRequested, bufferReady
}

func TestMetricBufferFlushesOnOverflow(t *testing.T) {
	// Sends metrics forever without requesting a flush, ensure a flush still occurs.
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// unbuffered queue to make sure things are processed in a predictable order
	mq, _, bufReady := makeTestMetricBuffer(ctx, 10*time.Second, 10, &gostatsd.TimerSubtypes{})

	mm := &gostatsd.MetricMap{
		Gauges: gostatsd.Gauges{
			"metric": map[string]gostatsd.Gauge{
				"key.value.s.hostname": {
					Value:     1,
					Timestamp: 0,
					Hostname:  "hostname",
					Tags: gostatsd.Tags{
						"key:value",
					},
				},
			},
		},
	}

	bm := &batchMessage{
		mm,
		time.Now(),
		func(err []error) {},
	}

	for {
		select {
		case <-ctx.Done():
			require.Fail(t, "timeout waiting for flush")
		case mq <- bm:
			// submitted
		case b := <-bufReady:
			require.EqualValues(t, 10, len(b["metric"]["key.value.s.hostname"].Points), "unexpected point count")
			require.EqualValues(t, 10, cap(b["metric"]["key.value.s.hostname"].Points), "unexpected point cap")
			return
		}
	}

}

func TestMetricBufferCallsDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// unbuffered queue to make sure things are processed in a predictable order
	mq, bufReq, bufReady := makeTestMetricBuffer(ctx, 10*time.Second, 0, &gostatsd.TimerSubtypes{})

	mm := &gostatsd.MetricMap{
		Gauges: gostatsd.Gauges{
			"metric": map[string]gostatsd.Gauge{
				"key.value.s.hostname": {
					Value:     1,
					Timestamp: 0,
					Hostname:  "hostname",
					Tags: gostatsd.Tags{
						"key:value",
					},
				},
			},
		},
	}

	called := int64(0)
	bm := &batchMessage{
		metrics:   mm,
		flushTime: time.Now(),
		done: func(err []error) {
			atomic.AddInt64(&called, 1)
		},
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout sending batch")
	case mq <- bm:
	}

	// perform a sync request, to ensure the batch is processed
	select {
	case <-ctx.Done():
		require.Fail(t, "timeout requesting response")
	case bufReq <- plz:
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout getting response")
	case <-bufReady:
	}

	require.EqualValues(t, 1, atomic.LoadInt64(&called), "done was not called")
}

func TestMetricBufferPercentiles(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// unbuffered queue to make sure things are processed in a predictable order
	mq, bufReq, bufReady := makeTestMetricBuffer(ctx, 10*time.Second, 0, &gostatsd.TimerSubtypes{})
	mm := &gostatsd.MetricMap{
		Timers: gostatsd.Timers{
			"t": map[string]gostatsd.Timer{ // Does not test percentiles
				"key.value.s.hostname": {
					Count:      5,
					PerSecond:  0.5,
					Mean:       4,
					Median:     4,
					Min:        2,
					Max:        6,
					StdDev:     1.4,
					Sum:        20,
					SumSquares: 90,
					Hostname:   "hostname",
					Percentiles: gostatsd.Percentiles{
						gostatsd.Percentile{
							Float: 1.0,
							Str:   "upper_90",
						},
					},
					Tags: gostatsd.Tags{
						"key:value",
					},
				},
			},
		},
	}

	timeStamp := 20.0
	bm := &batchMessage{
		metrics:   mm,
		flushTime: time.Unix(int64(timeStamp), 0),
		done:      func(err []error) {},
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout sending batch")
	case mq <- bm:
	}

	// perform a sync request, to ensure the batch is processed
	select {
	case <-ctx.Done():
		require.Fail(t, "timeout requesting response")
	case bufReq <- plz:
	}

	expected := ddMetricMap{
		"t.count":       map[string]*ddMetric{"key.value.s.hostname": newGauge("t.count", "hostname", timeStamp, 5, "key:value")},
		"t.count_ps":    map[string]*ddMetric{"key.value.s.hostname": newRate("t.count_ps", "hostname", timeStamp, 0.5, "key:value")},
		"t.lower":       map[string]*ddMetric{"key.value.s.hostname": newGauge("t.lower", "hostname", timeStamp, 2, "key:value")},
		"t.mean":        map[string]*ddMetric{"key.value.s.hostname": newGauge("t.mean", "hostname", timeStamp, 4, "key:value")},
		"t.median":      map[string]*ddMetric{"key.value.s.hostname": newGauge("t.median", "hostname", timeStamp, 4, "key:value")},
		"t.std":         map[string]*ddMetric{"key.value.s.hostname": newGauge("t.std", "hostname", timeStamp, 1.4, "key:value")},
		"t.sum":         map[string]*ddMetric{"key.value.s.hostname": newGauge("t.sum", "hostname", timeStamp, 20, "key:value")},
		"t.sum_squares": map[string]*ddMetric{"key.value.s.hostname": newGauge("t.sum_squares", "hostname", timeStamp, 90, "key:value")},
		"t.upper":       map[string]*ddMetric{"key.value.s.hostname": newGauge("t.upper", "hostname", timeStamp, 6, "key:value")},
		"t.upper_90":    map[string]*ddMetric{"key.value.s.hostname": newGauge("t.upper_90", "hostname", timeStamp, 1, "key:value")},
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout getting response")
	case result := <-bufReady:
		require.Equal(t, expected, result, "result mismatch")
	}
}

func TestMetricBufferHandlesAllMetricTypes(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// unbuffered queue to make sure things are processed in a predictable order
	mq, bufReq, bufReady := makeTestMetricBuffer(ctx, 10*time.Second, 0, &gostatsd.TimerSubtypes{})

	mm := &gostatsd.MetricMap{
		Counters: gostatsd.Counters{
			"c": map[string]gostatsd.Counter{
				"key.value.s.hostname": {
					Value:     1,
					PerSecond: 0.1,
					Hostname:  "hostname",
					Tags: gostatsd.Tags{
						"key:value",
					},
				},
			},
		},
		Timers: gostatsd.Timers{
			"t": map[string]gostatsd.Timer{ // Does not test percentiles
				"key.value.s.hostname": {
					Count:      5,
					PerSecond:  0.5,
					Mean:       4,
					Median:     4,
					Min:        2,
					Max:        6,
					StdDev:     1.4,
					Sum:        20,
					SumSquares: 90,
					Hostname:   "hostname",
					Tags: gostatsd.Tags{
						"key:value",
					},
				},
			},
		},
		Gauges: gostatsd.Gauges{
			"g": map[string]gostatsd.Gauge{
				"key.value.s.hostname": {
					Value:    7,
					Hostname: "hostname",
					Tags: gostatsd.Tags{
						"key:value",
					},
				},
			},
		},
		Sets: gostatsd.Sets{
			"s": map[string]gostatsd.Set{
				"key.value.s.hostname": {
					Values: map[string]struct{}{
						"alice":   {},
						"bob":     {},
						"carol":   {},
						"carlos":  {},
						"charlie": {},
						"chuck":   {},
						"craig":   {},
						"dan":     {},
					},
					Hostname: "hostname",
					Tags: gostatsd.Tags{
						"key:value",
					},
				},
			},
		},
	}

	timeStamp := 20.0
	bm := &batchMessage{
		metrics:   mm,
		flushTime: time.Unix(int64(timeStamp), 0),
		done:      func(err []error) {},
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout sending batch")
	case mq <- bm:
	}

	// perform a sync request, to ensure the batch is processed
	select {
	case <-ctx.Done():
		require.Fail(t, "timeout requesting response")
	case bufReq <- plz:
	}

	expected := ddMetricMap{
		"c":             map[string]*ddMetric{"key.value.s.hostname": newRate("c", "hostname", timeStamp, 0.1, "key:value")},
		"c.count":       map[string]*ddMetric{"key.value.s.hostname": newGauge("c.count", "hostname", timeStamp, 1, "key:value")},
		"t.count":       map[string]*ddMetric{"key.value.s.hostname": newGauge("t.count", "hostname", timeStamp, 5, "key:value")},
		"t.count_ps":    map[string]*ddMetric{"key.value.s.hostname": newRate("t.count_ps", "hostname", timeStamp, 0.5, "key:value")},
		"t.lower":       map[string]*ddMetric{"key.value.s.hostname": newGauge("t.lower", "hostname", timeStamp, 2, "key:value")},
		"t.mean":        map[string]*ddMetric{"key.value.s.hostname": newGauge("t.mean", "hostname", timeStamp, 4, "key:value")},
		"t.median":      map[string]*ddMetric{"key.value.s.hostname": newGauge("t.median", "hostname", timeStamp, 4, "key:value")},
		"t.std":         map[string]*ddMetric{"key.value.s.hostname": newGauge("t.std", "hostname", timeStamp, 1.4, "key:value")},
		"t.sum":         map[string]*ddMetric{"key.value.s.hostname": newGauge("t.sum", "hostname", timeStamp, 20, "key:value")},
		"t.sum_squares": map[string]*ddMetric{"key.value.s.hostname": newGauge("t.sum_squares", "hostname", timeStamp, 90, "key:value")},
		"t.upper":       map[string]*ddMetric{"key.value.s.hostname": newGauge("t.upper", "hostname", timeStamp, 6, "key:value")},
		"g":             map[string]*ddMetric{"key.value.s.hostname": newGauge("g", "hostname", timeStamp, 7, "key:value")},
		"s":             map[string]*ddMetric{"key.value.s.hostname": newGauge("s", "hostname", timeStamp, 8, "key:value")},
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout getting response")
	case result := <-bufReady:
		require.Equal(t, expected, result, "result mismatch")
	}
}

func TestMetricBufferHandlesMultipleTags(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// unbuffered queue to make sure things are processed in a predictable order
	mq, bufReq, bufReady := makeTestMetricBuffer(ctx, 10*time.Second, 0, &gostatsd.TimerSubtypes{})

	mm := &gostatsd.MetricMap{
		Gauges: gostatsd.Gauges{
			"g1": map[string]gostatsd.Gauge{
				"key.value1.s.hostname1": {
					Value:    111,
					Hostname: "hostname1",
					Tags: gostatsd.Tags{
						"key:value1",
					},
				},
				"key.value1.s.hostname2": {
					Value:    112,
					Hostname: "hostname2",
					Tags: gostatsd.Tags{
						"key:value1",
					},
				},
				"key.value2.s.hostname1": {
					Value:    121,
					Hostname: "hostname1",
					Tags: gostatsd.Tags{
						"key:value2",
					},
				},
				"key.value2.s.hostname2": {
					Value:    122,
					Hostname: "hostname2",
					Tags: gostatsd.Tags{
						"key:value2",
					},
				},
			},
			"g2": map[string]gostatsd.Gauge{
				"key.value1.s.hostname1": {
					Value:    211,
					Hostname: "hostname1",
					Tags: gostatsd.Tags{
						"key:value1",
					},
				},
				"key.value1.s.hostname2": {
					Value:    212,
					Hostname: "hostname2",
					Tags: gostatsd.Tags{
						"key:value1",
					},
				},
				"key.value2.s.hostname1": {
					Value:    221,
					Hostname: "hostname1",
					Tags: gostatsd.Tags{
						"key:value2",
					},
				},
				"key.value2.s.hostname2": {
					Value:    222,
					Hostname: "hostname2",
					Tags: gostatsd.Tags{
						"key:value2",
					},
				},
			},
		},
	}

	timeStamp := 20.0
	bm := &batchMessage{
		metrics:   mm,
		flushTime: time.Unix(int64(timeStamp), 0),
		done:      func(err []error) {},
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout sending batch")
	case mq <- bm:
	}

	// perform a sync request, to ensure the batch is processed
	select {
	case <-ctx.Done():
		require.Fail(t, "timeout requesting response")
	case bufReq <- plz:
	}

	expected := ddMetricMap{
		"g1": map[string]*ddMetric{
			"key.value1.s.hostname1": newGauge("g1", "hostname1", timeStamp, 111, "key:value1"),
			"key.value1.s.hostname2": newGauge("g1", "hostname2", timeStamp, 112, "key:value1"),
			"key.value2.s.hostname1": newGauge("g1", "hostname1", timeStamp, 121, "key:value2"),
			"key.value2.s.hostname2": newGauge("g1", "hostname2", timeStamp, 122, "key:value2"),
		},
		"g2": map[string]*ddMetric{
			"key.value1.s.hostname1": newGauge("g2", "hostname1", timeStamp, 211, "key:value1"),
			"key.value1.s.hostname2": newGauge("g2", "hostname2", timeStamp, 212, "key:value1"),
			"key.value2.s.hostname1": newGauge("g2", "hostname1", timeStamp, 221, "key:value2"),
			"key.value2.s.hostname2": newGauge("g2", "hostname2", timeStamp, 222, "key:value2"),
		},
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout getting response")
	case result := <-bufReady:
		require.Equal(t, expected, result, "result mismatch")
	}
}

func TestMetricBufferObeysDisabledMetrics(t *testing.T) {
	tests := []struct {
		subType  string
		disabled gostatsd.TimerSubtypes
	}{
		{"lower", gostatsd.TimerSubtypes{Lower: true}},
		{"upper", gostatsd.TimerSubtypes{Upper: true}},
		{"count", gostatsd.TimerSubtypes{Count: true}},
		{"count_ps", gostatsd.TimerSubtypes{CountPerSecond: true}},
		{"mean", gostatsd.TimerSubtypes{Mean: true}},
		{"median", gostatsd.TimerSubtypes{Median: true}},
		{"std", gostatsd.TimerSubtypes{StdDev: true}},
		{"sum", gostatsd.TimerSubtypes{Sum: true}},
		{"sum_squares", gostatsd.TimerSubtypes{SumSquares: true}},
	}

	for _, test := range tests {
		t.Run(test.subType, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			// unbuffered queue to make sure things are processed in a predictable order
			mq, bufReq, bufReady := makeTestMetricBuffer(ctx, 10*time.Second, 0, &test.disabled)

			mm := &gostatsd.MetricMap{
				Timers: gostatsd.Timers{
					"t": map[string]gostatsd.Timer{
						"key.value.s.hostname": {
							Count:      5,
							PerSecond:  0.5,
							Mean:       4,
							Median:     4,
							Min:        2,
							Max:        6,
							StdDev:     1.4,
							Sum:        20,
							SumSquares: 90,
							Hostname:   "hostname",
							Tags: gostatsd.Tags{
								"key:value",
							},
						},
					},
				},
			}
			timeStamp := 20.0
			bm := &batchMessage{
				metrics:   mm,
				flushTime: time.Unix(int64(timeStamp), 0),
				done:      func(err []error) {},
			}

			select {
			case <-ctx.Done():
				require.Fail(t, "timeout sending batch")
			case mq <- bm:
			}

			// perform a sync request, to ensure the batch is processed
			select {
			case <-ctx.Done():
				require.Fail(t, "timeout requesting response")
			case bufReq <- plz:
			}

			expected := ddMetricMap{
				"t.count":       map[string]*ddMetric{"key.value.s.hostname": newGauge("t.count", "hostname", timeStamp, 5, "key:value")},
				"t.count_ps":    map[string]*ddMetric{"key.value.s.hostname": newRate("t.count_ps", "hostname", timeStamp, 0.5, "key:value")},
				"t.lower":       map[string]*ddMetric{"key.value.s.hostname": newGauge("t.lower", "hostname", timeStamp, 2, "key:value")},
				"t.mean":        map[string]*ddMetric{"key.value.s.hostname": newGauge("t.mean", "hostname", timeStamp, 4, "key:value")},
				"t.median":      map[string]*ddMetric{"key.value.s.hostname": newGauge("t.median", "hostname", timeStamp, 4, "key:value")},
				"t.std":         map[string]*ddMetric{"key.value.s.hostname": newGauge("t.std", "hostname", timeStamp, 1.4, "key:value")},
				"t.sum":         map[string]*ddMetric{"key.value.s.hostname": newGauge("t.sum", "hostname", timeStamp, 20, "key:value")},
				"t.sum_squares": map[string]*ddMetric{"key.value.s.hostname": newGauge("t.sum_squares", "hostname", timeStamp, 90, "key:value")},
				"t.upper":       map[string]*ddMetric{"key.value.s.hostname": newGauge("t.upper", "hostname", timeStamp, 6, "key:value")},
			}

			// Remove the thing we expect missing
			delete(expected, "t."+test.subType)

			select {
			case <-ctx.Done():
				require.Fail(t, "timeout getting response")
			case result := <-bufReady:
				require.Equal(t, expected, result, "result mismatch")
			}

		})
	}
}

func newRate(name, hostname string, timeStamp, value float64, tags ...string) *ddMetric {
	return &ddMetric{
		Metric:   name,
		Host:     hostname,
		Interval: 10.0,
		Points:   []ddPoint{{timeStamp, value}},
		Tags:     tags,
		Type:     rate,
	}
}

func newGauge(name, hostname string, timeStamp, value float64, tags ...string) *ddMetric {
	return &ddMetric{
		Metric:   name,
		Host:     hostname,
		Interval: 10.0,
		Points:   []ddPoint{{timeStamp, value}},
		Tags:     tags,
		Type:     gauge,
	}
}
