package statsd

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/ash2k/stager"
	"github.com/stretchr/testify/assert"

	"github.com/atlassian/gostatsd"
)

func newFakeAggregator() *MetricAggregator {
	return NewMetricAggregator(
		[]float64{90},
		5*time.Minute,
		gostatsd.TimerSubtypes{},
	)
}

type fakeAggregatorFactory struct{}

func (faf *fakeAggregatorFactory) Create() Aggregator {
	return newFakeAggregator()
}

func TestNewAggregator(t *testing.T) {
	t.Parallel()
	assrt := assert.New(t)

	actual := newFakeAggregator()

	if assrt.NotNil(actual.metricMap.Counters) {
		assrt.Equal(gostatsd.Counters{}, actual.metricMap.Counters)
	}

	if assrt.NotNil(actual.metricMap.Timers) {
		assrt.Equal(gostatsd.Timers{}, actual.metricMap.Timers)
	}

	if assrt.NotNil(actual.metricMap.Gauges) {
		assrt.Equal(gostatsd.Gauges{}, actual.metricMap.Gauges)
	}

	if assrt.NotNil(actual.metricMap.Sets) {
		assrt.Equal(gostatsd.Sets{}, actual.metricMap.Sets)
	}
}

func TestFlush(t *testing.T) {
	t.Parallel()
	assrt := assert.New(t)

	now := time.Now()
	nowFn := func() time.Time { return now }
	ma := newFakeAggregator()
	ma.now = nowFn
	expected := newFakeAggregator()
	expected.now = nowFn

	ma.metricMap.Counters["some"] = make(map[string]gostatsd.Counter)
	ma.metricMap.Counters["some"][""] = gostatsd.Counter{Value: 50}
	ma.metricMap.Counters["some"]["thing"] = gostatsd.Counter{Value: 100}
	ma.metricMap.Counters["some"]["other:thing"] = gostatsd.Counter{Value: 150}

	expected.metricMap.Counters["some"] = make(map[string]gostatsd.Counter)
	expected.metricMap.Counters["some"][""] = gostatsd.Counter{Value: 50, PerSecond: 5}
	expected.metricMap.Counters["some"]["thing"] = gostatsd.Counter{Value: 100, PerSecond: 10}
	expected.metricMap.Counters["some"]["other:thing"] = gostatsd.Counter{Value: 150, PerSecond: 15}

	ma.metricMap.Timers["some"] = make(map[string]gostatsd.Timer)
	ma.metricMap.Timers["some"]["thing"] = gostatsd.NewTimerValues([]float64{2, 4, 12})
	ma.metricMap.Timers["some"]["sampled"] = gostatsd.Timer{Values: []float64{2, 4, 12}, SampledCount: 30.0}
	ma.metricMap.Timers["some"]["empty"] = gostatsd.Timer{Values: []float64{}}

	expPct := gostatsd.Percentiles{}
	expPct.Set("count_90", float64(3))
	expPct.Set("mean_90", float64(6))
	expPct.Set("sum_90", float64(18))
	expPct.Set("sum_squares_90", float64(164))
	expPct.Set("upper_90", float64(12))
	expected.metricMap.Timers["some"] = make(map[string]gostatsd.Timer)
	expected.metricMap.Timers["some"]["thing"] = gostatsd.Timer{
		Values: []float64{2, 4, 12}, Count: 3, Min: 2, Max: 12, Mean: 6, Median: 4, Sum: 18,
		PerSecond: 0.3, SumSquares: 164, StdDev: 4.320493798938574, Percentiles: expPct,
		SampledCount: 3.0,
	}
	expected.metricMap.Timers["some"]["sampled"] = gostatsd.Timer{
		Values: []float64{2, 4, 12}, Count: 30, Min: 2, Max: 12, Mean: 6, Median: 4, Sum: 18,
		PerSecond: 3.0, SumSquares: 164, StdDev: 4.320493798938574, Percentiles: expPct,
		SampledCount: 30.0,
	}
	expected.metricMap.Timers["some"]["empty"] = gostatsd.Timer{Values: []float64{}}

	ma.metricMap.Gauges["some"] = make(map[string]gostatsd.Gauge)
	ma.metricMap.Gauges["some"][""] = gostatsd.Gauge{Value: 50}
	ma.metricMap.Gauges["some"]["thing"] = gostatsd.Gauge{Value: 100}
	ma.metricMap.Gauges["some"]["other:thing"] = gostatsd.Gauge{Value: 150}

	expected.metricMap.Gauges["some"] = make(map[string]gostatsd.Gauge)
	expected.metricMap.Gauges["some"][""] = gostatsd.Gauge{Value: 50}
	expected.metricMap.Gauges["some"]["thing"] = gostatsd.Gauge{Value: 100}
	expected.metricMap.Gauges["some"]["other:thing"] = gostatsd.Gauge{Value: 150}

	ma.metricMap.Sets["some"] = make(map[string]gostatsd.Set)
	unique := map[string]struct{}{
		"user": {},
	}
	ma.metricMap.Sets["some"]["thing"] = gostatsd.Set{Values: unique}

	expected.metricMap.Sets["some"] = make(map[string]gostatsd.Set)
	expected.metricMap.Sets["some"]["thing"] = gostatsd.Set{Values: unique}

	ma.Flush(10 * time.Second)
	assrt.Equal(expected.metricMap.Counters, ma.metricMap.Counters)
	assrt.Equal(expected.metricMap.Timers, ma.metricMap.Timers)
	assrt.Equal(expected.metricMap.Gauges, ma.metricMap.Gauges)
	assrt.Equal(expected.metricMap.Sets, ma.metricMap.Sets)
}

func BenchmarkFlush(b *testing.B) {
	ma := newFakeAggregator()
	ma.metricMap.Counters["some"] = make(map[string]gostatsd.Counter)
	ma.metricMap.Counters["some"][""] = gostatsd.Counter{Value: 50}
	ma.metricMap.Counters["some"]["thing"] = gostatsd.Counter{Value: 100}
	ma.metricMap.Counters["some"]["other:thing"] = gostatsd.Counter{Value: 150}

	ma.metricMap.Timers["some"] = make(map[string]gostatsd.Timer)
	ma.metricMap.Timers["some"]["thing"] = gostatsd.NewTimerValues([]float64{2, 4, 12})
	ma.metricMap.Timers["some"]["empty"] = gostatsd.Timer{Values: []float64{}}

	ma.metricMap.Gauges["some"] = make(map[string]gostatsd.Gauge)
	ma.metricMap.Gauges["some"][""] = gostatsd.Gauge{Value: 50}
	ma.metricMap.Gauges["some"]["thing"] = gostatsd.Gauge{Value: 100}
	ma.metricMap.Gauges["some"]["other:thing"] = gostatsd.Gauge{Value: 150}

	ma.metricMap.Sets["some"] = make(map[string]gostatsd.Set)
	unique := map[string]struct{}{
		"user": {},
	}
	ma.metricMap.Sets["some"]["thing"] = gostatsd.Set{Values: unique}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ma.Flush(1 * time.Second)
	}
}

func TestReset(t *testing.T) {
	t.Parallel()
	assrt := assert.New(t)
	now := time.Now()
	nowNano := gostatsd.Nanotime(now.UnixNano())
	nowFn := func() time.Time { return now }
	host := "hostname"

	// non expired
	actual := newFakeAggregator()
	actual.metricMap.Counters["some"] = map[string]gostatsd.Counter{
		"thing":       gostatsd.NewCounter(nowNano, 50, host, nil),
		"other:thing": gostatsd.NewCounter(nowNano, 90, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected := newFakeAggregator()
	expected.metricMap.Counters["some"] = map[string]gostatsd.Counter{
		"thing":       gostatsd.NewCounter(nowNano, 0, host, nil),
		"other:thing": gostatsd.NewCounter(nowNano, 0, host, nil),
	}
	expected.now = nowFn

	assrt.Equal(expected.metricMap.Counters, actual.metricMap.Counters)

	actual = newFakeAggregator()
	actual.metricMap.Timers["some"] = map[string]gostatsd.Timer{
		"thing": gostatsd.NewTimer(nowNano, []float64{50}, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected = newFakeAggregator()
	expected.metricMap.Timers["some"] = map[string]gostatsd.Timer{
		"thing": gostatsd.NewTimer(nowNano, []float64{}, host, nil),
	}
	expected.now = nowFn

	assrt.Equal(expected.metricMap.Timers, actual.metricMap.Timers)

	actual = newFakeAggregator()
	actual.metricMap.Gauges["some"] = map[string]gostatsd.Gauge{
		"thing":       gostatsd.NewGauge(nowNano, 50, host, nil),
		"other:thing": gostatsd.NewGauge(nowNano, 90, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected = newFakeAggregator()
	expected.metricMap.Gauges["some"] = map[string]gostatsd.Gauge{
		"thing":       gostatsd.NewGauge(nowNano, 50, host, nil),
		"other:thing": gostatsd.NewGauge(nowNano, 90, host, nil),
	}
	expected.now = nowFn

	assrt.Equal(expected.metricMap.Gauges, actual.metricMap.Gauges)

	actual = newFakeAggregator()
	actual.metricMap.Sets["some"] = map[string]gostatsd.Set{
		"thing": gostatsd.NewSet(nowNano, map[string]struct{}{"user": {}}, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected = newFakeAggregator()
	expected.metricMap.Sets["some"] = map[string]gostatsd.Set{
		"thing": gostatsd.NewSet(nowNano, make(map[string]struct{}), host, nil),
	}
	expected.now = nowFn

	assrt.Equal(expected.metricMap.Sets, actual.metricMap.Sets)

	// expired
	pastNano := gostatsd.Nanotime(now.Add(-30 * time.Second).UnixNano())

	actual = newFakeAggregator()
	actual.expiryInterval = 10 * time.Second
	actual.metricMap.Counters["some"] = map[string]gostatsd.Counter{
		"thing":       gostatsd.NewCounter(pastNano, 50, host, nil),
		"other:thing": gostatsd.NewCounter(pastNano, 90, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected = newFakeAggregator()
	expected.now = nowFn

	assrt.Equal(expected.metricMap.Counters, actual.metricMap.Counters)

	actual = newFakeAggregator()
	actual.expiryInterval = 10 * time.Second
	actual.metricMap.Timers["some"] = map[string]gostatsd.Timer{
		"thing": gostatsd.NewTimer(pastNano, []float64{50}, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected = newFakeAggregator()
	expected.now = nowFn

	assrt.Equal(expected.metricMap.Timers, actual.metricMap.Timers)

	actual = newFakeAggregator()
	actual.expiryInterval = 10 * time.Second
	actual.metricMap.Gauges["some"] = map[string]gostatsd.Gauge{
		"thing":       gostatsd.NewGauge(pastNano, 50, host, nil),
		"other:thing": gostatsd.NewGauge(pastNano, 90, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected = newFakeAggregator()
	expected.now = nowFn

	assrt.Equal(expected.metricMap.Gauges, actual.metricMap.Gauges)

	actual = newFakeAggregator()
	actual.expiryInterval = 10 * time.Second
	actual.metricMap.Sets["some"] = map[string]gostatsd.Set{
		"thing": gostatsd.NewSet(pastNano, map[string]struct{}{"user": {}}, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected = newFakeAggregator()
	expected.now = nowFn

	assrt.Equal(expected.metricMap.Sets, actual.metricMap.Sets)
}

func TestIsExpired(t *testing.T) {
	t.Parallel()
	assrt := assert.New(t)

	now := gostatsd.Nanotime(time.Now().UnixNano())

	ma := &MetricAggregator{expiryInterval: 0}
	assrt.Equal(false, ma.isExpired(now, now))

	ma.expiryInterval = 10 * time.Second

	ts := gostatsd.Nanotime(time.Now().Add(-30 * time.Second).UnixNano())
	assrt.Equal(true, ma.isExpired(now, ts))

	ts = gostatsd.Nanotime(time.Now().Add(-1 * time.Second).UnixNano())
	assrt.Equal(false, ma.isExpired(now, ts))
}

func TestDisabledCount(t *testing.T) {
	t.Parallel()
	ma := newFakeAggregator()
	ma.disabledSubtypes.CountPct = true
	ma.Receive(&gostatsd.Metric{Name: "x", Value: 1, Type: gostatsd.TIMER}, time.Now())
	ma.Flush(1 * time.Second)
	for _, pct := range ma.metricMap.Timers["x"][""].Percentiles {
		if pct.Str == "count_90" {
			t.Error("count not disabled")
		}
	}
}

func TestDisabledMean(t *testing.T) {
	t.Parallel()
	ma := newFakeAggregator()
	ma.disabledSubtypes.MeanPct = true
	ma.Receive(&gostatsd.Metric{Name: "x", Value: 1, Type: gostatsd.TIMER}, time.Now())
	ma.Flush(1 * time.Second)
	for _, pct := range ma.metricMap.Timers["x"][""].Percentiles {
		if pct.Str == "mean_90" {
			t.Error("mean not disabled")
		}
	}
}

func TestDisabledSum(t *testing.T) {
	t.Parallel()
	ma := newFakeAggregator()
	ma.disabledSubtypes.SumPct = true
	ma.Receive(&gostatsd.Metric{Name: "x", Value: 1, Type: gostatsd.TIMER}, time.Now())
	ma.Flush(1 * time.Second)
	for _, pct := range ma.metricMap.Timers["x"][""].Percentiles {
		if pct.Str == "sum_90" {
			t.Error("sum not disabled")
		}
	}
}

func TestDisabledSumSquares(t *testing.T) {
	t.Parallel()
	ma := newFakeAggregator()
	ma.disabledSubtypes.SumSquaresPct = true
	ma.Receive(&gostatsd.Metric{Name: "x", Value: 1, Type: gostatsd.TIMER}, time.Now())
	ma.Flush(1 * time.Second)
	for _, pct := range ma.metricMap.Timers["x"][""].Percentiles {
		if pct.Str == "sum_squares_90" {
			t.Error("sum_squares not disabled")
		}
	}
}

func TestDisabledUpper(t *testing.T) {
	t.Parallel()
	ma := newFakeAggregator()
	ma.disabledSubtypes.UpperPct = true
	ma.Receive(&gostatsd.Metric{Name: "x", Value: 1, Type: gostatsd.TIMER}, time.Now())
	ma.Flush(1 * time.Second)
	for _, pct := range ma.metricMap.Timers["x"][""].Percentiles {
		if pct.Str == "upper_90" {
			t.Error("upper not disabled")
		}
	}
}

func TestDisabledLower(t *testing.T) {
	t.Parallel()
	ma := NewMetricAggregator(
		[]float64{-90},
		5*time.Minute,
		gostatsd.TimerSubtypes{},
	)
	ma.disabledSubtypes.LowerPct = true
	ma.Receive(&gostatsd.Metric{Name: "x", Value: 1, Type: gostatsd.TIMER}, time.Now())
	ma.Flush(1 * time.Second)
	for _, pct := range ma.metricMap.Timers["x"][""].Percentiles {
		if pct.Str == "lower_-90" { // lower_-90?
			t.Error("lower not disabled")
		}
	}
}

func BenchmarkHotMetric(b *testing.B) {
	beh := NewBackendHandler(
		nil,
		1000,
		runtime.NumCPU(),
		10000,
		&fakeAggregatorFactory{},
	)

	stgr := stager.New()
	stage := stgr.NextStage()
	stage.StartWithContext(beh.Run)
	stage = stgr.NextStage()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < runtime.NumCPU(); i++ {
		stage.Start(func() {
			for n := 0; n < b.N; n++ {
				m := &gostatsd.Metric{
					Name:     "metric.name",
					Value:    5,
					Tags:     gostatsd.Tags{"aaaa:aaaa", "aaab:aaab", "aaac:aaac", "aaad:aaad", "aaae:aaae", "aaaf:aaaf"},
					Hostname: "local",
					Type:     gostatsd.GAUGE,
				}
				beh.DispatchMetric(ctx, m)
			}
		})
	}

	stgr.Shutdown()
}
