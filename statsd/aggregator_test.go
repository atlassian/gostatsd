package statsd

import (
	"testing"
	"time"

	"github.com/atlassian/gostatsd/types"

	"github.com/stretchr/testify/assert"
)

type FakeBackend struct{}

func (fb *FakeBackend) SendMetrics(metrics *types.MetricMap) error { return nil }
func (fb *FakeBackend) SampleConfig() string                       { return "" }
func (fb *FakeBackend) BackendName() string                        { return "fake" }

func newFakeAggregator() *aggregator {
	return NewAggregator(
		[]float64{float64(90)},
		time.Duration(10)*time.Second,
		time.Duration(5)*time.Minute,
		[]string{},
	).(*aggregator)
}

func TestNewAggregator(t *testing.T) {
	assert := assert.New(t)

	actual := newFakeAggregator()

	if assert.NotNil(actual.Counters) {
		assert.Equal(types.Counters{}, actual.Counters)
	}

	if assert.NotNil(actual.Timers) {
		assert.Equal(types.Timers{}, actual.Timers)
	}

	if assert.NotNil(actual.Gauges) {
		assert.Equal(types.Gauges{}, actual.Gauges)
	}

	if assert.NotNil(actual.Sets) {
		assert.Equal(types.Sets{}, actual.Sets)
	}
}

func TestFlush(t *testing.T) {
	assert := assert.New(t)

	now := time.Now()
	nowFn := func() time.Time { return now }
	ma := newFakeAggregator()
	expected := newFakeAggregator()
	ma.lastFlush = now.Add(-10 * time.Second)
	expected.lastFlush = now.Add(-10 * time.Second)

	ma.Counters["some"] = make(map[string]types.Counter)
	ma.Counters["some"][""] = types.Counter{Value: 50}
	ma.Counters["some"]["thing"] = types.Counter{Value: 100}
	ma.Counters["some"]["other:thing"] = types.Counter{Value: 150}

	expected.Counters["some"] = make(map[string]types.Counter)
	expected.Counters["some"][""] = types.Counter{Value: 50, PerSecond: 5}
	expected.Counters["some"]["thing"] = types.Counter{Value: 100, PerSecond: 10}
	expected.Counters["some"]["other:thing"] = types.Counter{Value: 150, PerSecond: 15}
	expected.Counters["statsd.aggregator_num_stats"] = make(map[string]types.Counter)
	expected.Counters["statsd.aggregator_num_stats"][""] = types.Counter{
		Value: 0, PerSecond: 0,
		Interval: types.Interval{Timestamp: now, Flush: time.Duration(10) * time.Second},
	} // count happens in Receive

	ma.Timers["some"] = make(map[string]types.Timer)
	ma.Timers["some"]["thing"] = types.Timer{Values: []float64{2, 4, 12}}
	ma.Timers["some"]["empty"] = types.Timer{Values: []float64{}}

	expPct := types.Percentiles{}
	expPct.Set("count_90", float64(3))
	expPct.Set("mean_90", float64(6))
	expPct.Set("sum_90", float64(18))
	expPct.Set("sum_squares_90", float64(164))
	expPct.Set("upper_90", float64(12))
	expected.Timers["some"] = make(map[string]types.Timer)
	expected.Timers["some"]["thing"] = types.Timer{
		Values: []float64{2, 4, 12}, Count: 3, Min: 2, Max: 12, Mean: 6, Median: 4, Sum: 18,
		PerSecond: 0.3, SumSquares: 164, StdDev: 4.320493798938574, Percentiles: expPct,
	}
	expected.Timers["some"]["empty"] = types.Timer{Values: []float64{}}

	ma.Gauges["some"] = make(map[string]types.Gauge)
	ma.Gauges["some"][""] = types.Gauge{Value: 50}
	ma.Gauges["some"]["thing"] = types.Gauge{Value: 100}
	ma.Gauges["some"]["other:thing"] = types.Gauge{Value: 150}

	expected.Gauges["some"] = make(map[string]types.Gauge)
	expected.Gauges["some"][""] = types.Gauge{Value: 50}
	expected.Gauges["some"]["thing"] = types.Gauge{Value: 100}
	expected.Gauges["some"]["other:thing"] = types.Gauge{Value: 150}
	expected.Gauges["statsd.processing_time"] = make(map[string]types.Gauge)
	expected.Gauges["statsd.processing_time"][""] = types.Gauge{
		Value:    0,
		Interval: types.Interval{Timestamp: now, Flush: time.Duration(10) * time.Second},
	} // start and end are the same...

	ma.Sets["some"] = make(map[string]types.Set)
	unique := make(map[string]int64)
	unique["user"] = 1
	ma.Sets["some"]["thing"] = types.Set{Values: unique}

	expected.Sets["some"] = make(map[string]types.Set)
	expected.Sets["some"]["thing"] = types.Set{Values: unique}

	ma.Flush(nowFn)
	assert.Equal(expected.Counters, ma.Counters)
	assert.Equal(expected.Timers, ma.Timers)
	assert.Equal(expected.Gauges, ma.Gauges)
	assert.Equal(expected.Sets, ma.Sets)
}

func BenchmarkFlush(b *testing.B) {
	ma := newFakeAggregator()
	ma.Counters["some"] = make(map[string]types.Counter)
	ma.Counters["some"][""] = types.Counter{Value: 50}
	ma.Counters["some"]["thing"] = types.Counter{Value: 100}
	ma.Counters["some"]["other:thing"] = types.Counter{Value: 150}

	ma.Timers["some"] = make(map[string]types.Timer)
	ma.Timers["some"]["thing"] = types.Timer{Values: []float64{2, 4, 12}}
	ma.Timers["some"]["empty"] = types.Timer{Values: []float64{}}

	ma.Gauges["some"] = make(map[string]types.Gauge)
	ma.Gauges["some"][""] = types.Gauge{Value: 50}
	ma.Gauges["some"]["thing"] = types.Gauge{Value: 100}
	ma.Gauges["some"]["other:thing"] = types.Gauge{Value: 150}

	ma.Sets["some"] = make(map[string]types.Set)
	unique := make(map[string]int64)
	unique["user"] = 1
	ma.Sets["some"]["thing"] = types.Set{Values: unique}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ma.Flush(time.Now)
	}
}

func TestReset(t *testing.T) {
	assert := assert.New(t)
	now := time.Now()

	// non expired
	actual := newFakeAggregator()
	actual.Counters["some"] = make(map[string]types.Counter)
	actual.Counters["some"]["thing"] = types.NewCounter(now, time.Duration(10)*time.Second, int64(50))
	actual.Counters["some"]["other:thing"] = types.NewCounter(now, time.Duration(10)*time.Second, int64(90))
	actual.Reset(now)

	expected := newFakeAggregator()
	expected.Counters["some"] = make(map[string]types.Counter)
	expected.Counters["some"]["thing"] = types.NewCounter(now, time.Duration(10)*time.Second, int64(0))
	expected.Counters["some"]["other:thing"] = types.NewCounter(now, time.Duration(10)*time.Second, int64(0))

	assert.Equal(expected.Counters, actual.Counters)

	actual = newFakeAggregator()
	actual.Timers["some"] = make(map[string]types.Timer)
	actual.Timers["some"]["thing"] = types.NewTimer(now, time.Duration(10)*time.Second, []float64{50})
	actual.Reset(now)

	expected = newFakeAggregator()
	expected.Timers["some"] = make(map[string]types.Timer)
	expected.Timers["some"]["thing"] = types.Timer{Interval: types.Interval{Timestamp: now, Flush: time.Duration(10) * time.Second}}

	assert.Equal(expected.Timers, actual.Timers)

	actual = newFakeAggregator()
	actual.Gauges["some"] = make(map[string]types.Gauge)
	actual.Gauges["some"]["thing"] = types.NewGauge(now, time.Duration(10)*time.Second, float64(50))
	actual.Gauges["some"]["other:thing"] = types.NewGauge(now, time.Duration(10)*time.Second, float64(90))
	actual.Reset(now)

	expected = newFakeAggregator()
	expected.Gauges["some"] = make(map[string]types.Gauge)
	expected.Gauges["some"]["thing"] = types.NewGauge(now, time.Duration(10)*time.Second, float64(50))
	expected.Gauges["some"]["other:thing"] = types.NewGauge(now, time.Duration(10)*time.Second, float64(90))

	assert.Equal(expected.Gauges, actual.Gauges)

	actual = newFakeAggregator()
	actual.Sets["some"] = make(map[string]types.Set)
	unique := make(map[string]int64)
	unique["user"] = 1
	actual.Sets["some"]["thing"] = types.NewSet(now, time.Duration(10)*time.Second, unique)
	actual.Reset(now)

	expected = newFakeAggregator()
	expected.Sets["some"] = make(map[string]types.Set)
	expected.Sets["some"]["thing"] = types.NewSet(now, time.Duration(10)*time.Second, make(map[string]int64))

	assert.Equal(expected.Sets, actual.Sets)

	// expired
	past := now.Add(-30 * time.Second)

	actual = newFakeAggregator()
	actual.expiryInterval = time.Duration(10) * time.Second
	actual.Counters["some"] = make(map[string]types.Counter)
	actual.Counters["some"]["thing"] = types.NewCounter(past, time.Duration(10)*time.Second, int64(50))
	actual.Counters["some"]["other:thing"] = types.NewCounter(past, time.Duration(10)*time.Second, int64(90))
	actual.Reset(now)

	expected = newFakeAggregator()

	assert.Equal(expected.Counters, actual.Counters)

	actual = newFakeAggregator()
	actual.expiryInterval = time.Duration(10) * time.Second
	actual.Timers["some"] = make(map[string]types.Timer)
	actual.Timers["some"]["thing"] = types.NewTimer(past, time.Duration(10)*time.Second, []float64{50})
	actual.Reset(now)

	expected = newFakeAggregator()

	assert.Equal(expected.Timers, actual.Timers)

	actual = newFakeAggregator()
	actual.expiryInterval = time.Duration(10) * time.Second
	actual.Gauges["some"] = make(map[string]types.Gauge)
	actual.Gauges["some"]["thing"] = types.NewGauge(past, time.Duration(10)*time.Second, float64(50))
	actual.Gauges["some"]["other:thing"] = types.NewGauge(past, time.Duration(10)*time.Second, float64(90))
	actual.Reset(now)

	expected = newFakeAggregator()

	assert.Equal(expected.Gauges, actual.Gauges)

	actual = newFakeAggregator()
	actual.expiryInterval = time.Duration(10) * time.Second
	actual.Sets["some"] = make(map[string]types.Set)
	unique = make(map[string]int64)
	unique["user"] = 1
	actual.Sets["some"]["thing"] = types.NewSet(past, time.Duration(10)*time.Second, unique)
	actual.Reset(now)

	expected = newFakeAggregator()

	assert.Equal(expected.Sets, actual.Sets)
}

func TestIsExpired(t *testing.T) {
	assert := assert.New(t)

	now := time.Now()

	ma := &aggregator{expiryInterval: time.Duration(0)}
	assert.Equal(false, ma.isExpired(now, now))

	ma.expiryInterval = time.Duration(10) * time.Second

	ts := time.Now().Add(-30 * time.Second)
	assert.Equal(true, ma.isExpired(now, ts))

	ts = time.Now().Add(-1 * time.Second)
	assert.Equal(false, ma.isExpired(now, ts))
}

func metricsFixtures() []types.Metric {
	return []types.Metric{
		{Name: "foo.bar.baz", Value: 2, Type: types.COUNTER},
		{Name: "abc.def.g", Value: 3, Type: types.GAUGE},
		{Name: "abc.def.g", Value: 8, Type: types.GAUGE, Tags: types.Tags{"foo:bar", "baz"}},
		{Name: "def.g", Value: 10, Type: types.TIMER},
		{Name: "def.g", Value: 1, Type: types.TIMER, Tags: types.Tags{"foo:bar", "baz"}},
		{Name: "smp.rte", Value: 50, Type: types.COUNTER},
		{Name: "smp.rte", Value: 50, Type: types.COUNTER, Tags: types.Tags{"foo:bar", "baz"}},
		{Name: "smp.rte", Value: 5, Type: types.COUNTER, Tags: types.Tags{"foo:bar", "baz"}},
		{Name: "uniq.usr", StringValue: "joe", Type: types.SET},
		{Name: "uniq.usr", StringValue: "joe", Type: types.SET},
		{Name: "uniq.usr", StringValue: "bob", Type: types.SET},
		{Name: "uniq.usr", StringValue: "john", Type: types.SET},
		{Name: "uniq.usr", StringValue: "john", Type: types.SET, Tags: types.Tags{"foo:bar", "baz"}},
	}
}

func TestReceive(t *testing.T) {
	assert := assert.New(t)

	ma := newFakeAggregator()
	now := time.Now()
	d := time.Duration(10) * time.Second
	interval := types.Interval{Timestamp: now, Flush: d}

	tests := metricsFixtures()
	for _, metric := range tests {
		ma.Receive(&metric, now)
	}

	expectedCounters := types.Counters{}
	expectedCounters["foo.bar.baz"] = make(map[string]types.Counter)
	expectedCounters["foo.bar.baz"][""] = types.Counter{Value: 2, Interval: interval}
	expectedCounters["smp.rte"] = make(map[string]types.Counter)
	expectedCounters["smp.rte"]["baz,foo:bar"] = types.Counter{Value: 55, Interval: interval}
	expectedCounters["smp.rte"][""] = types.Counter{Value: 50, Interval: interval}
	assert.Equal(expectedCounters, ma.Counters)

	expectedGauges := types.Gauges{}
	expectedGauges["abc.def.g"] = make(map[string]types.Gauge)
	expectedGauges["abc.def.g"][""] = types.Gauge{Value: 3, Interval: interval}
	expectedGauges["abc.def.g"]["baz,foo:bar"] = types.Gauge{Value: 8, Interval: interval}
	assert.Equal(expectedGauges, ma.Gauges)

	expectedTimers := types.Timers{}
	expectedTimers["def.g"] = make(map[string]types.Timer)
	expectedTimers["def.g"][""] = types.Timer{Values: []float64{10}, Interval: interval}
	expectedTimers["def.g"]["baz,foo:bar"] = types.Timer{Values: []float64{1}, Interval: interval}
	assert.Equal(expectedTimers, ma.Timers)

	expectedSets := types.Sets{}
	expectedSets["uniq.usr"] = make(map[string]types.Set)
	sets := make(map[string]int64)
	sets["joe"] = 2
	sets["bob"] = 1
	sets["john"] = 1
	sets2 := make(map[string]int64)
	sets2["john"] = 1
	expectedSets["uniq.usr"][""] = types.Set{Values: sets, Interval: interval}
	expectedSets["uniq.usr"]["baz,foo:bar"] = types.Set{Values: sets2, Interval: interval}
	assert.Equal(expectedSets, ma.Sets)
}

func benchmarkReceive(metric types.Metric, b *testing.B) {
	ma := newFakeAggregator()
	now := time.Now()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ma.Receive(&metric, now)
	}
}

func BenchmarkReceiveCounter(b *testing.B) {
	benchmarkReceive(types.Metric{Name: "foo.bar.baz", Value: 2, Type: types.COUNTER}, b)
}
func BenchmarkReceiveGauge(b *testing.B) {
	benchmarkReceive(types.Metric{Name: "abc.def.g", Value: 3, Type: types.GAUGE}, b)
}
func BenchmarkReceiveTimer(b *testing.B) {
	benchmarkReceive(types.Metric{Name: "def.g", Value: 10, Type: types.TIMER}, b)
}
func BenchmarkReceiveSet(b *testing.B) {
	benchmarkReceive(types.Metric{Name: "uniq.usr", StringValue: "joe", Type: types.SET}, b)
}

func BenchmarkReceives(b *testing.B) {
	ma := newFakeAggregator()
	now := time.Now()
	tests := metricsFixtures()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, metric := range tests {
			ma.Receive(&metric, now)
		}
	}
}
