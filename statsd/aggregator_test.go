package statsd

import (
	"testing"
	"time"

	"github.com/atlassian/gostatsd/types"

	"github.com/stretchr/testify/assert"
)

func newFakeAggregator() *aggregator {
	return NewAggregator(
		[]float64{90},
		5*time.Minute,
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
	ma.now = nowFn
	expected := newFakeAggregator()
	expected.now = nowFn

	ma.Counters["some"] = make(map[string]types.Counter)
	ma.Counters["some"][""] = types.Counter{Value: 50}
	ma.Counters["some"]["thing"] = types.Counter{Value: 100}
	ma.Counters["some"]["other:thing"] = types.Counter{Value: 150}

	expected.Counters["some"] = make(map[string]types.Counter)
	expected.Counters["some"][""] = types.Counter{Value: 50, PerSecond: 5}
	expected.Counters["some"]["thing"] = types.Counter{Value: 100, PerSecond: 10}
	expected.Counters["some"]["other:thing"] = types.Counter{Value: 150, PerSecond: 15}

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

	ma.Sets["some"] = make(map[string]types.Set)
	unique := map[string]struct{}{
		"user": {},
	}
	ma.Sets["some"]["thing"] = types.Set{Values: unique}

	expected.Sets["some"] = make(map[string]types.Set)
	expected.Sets["some"]["thing"] = types.Set{Values: unique}

	ma.Flush(10 * time.Second)
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
	unique := map[string]struct{}{
		"user": {},
	}
	ma.Sets["some"]["thing"] = types.Set{Values: unique}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ma.Flush(1 * time.Second)
	}
}

func TestReset(t *testing.T) {
	assert := assert.New(t)
	now := time.Now()
	nowNano := types.Nanotime(now.UnixNano())
	nowFn := func() time.Time { return now }
	host := "hostname"

	// non expired
	actual := newFakeAggregator()
	actual.Counters["some"] = map[string]types.Counter{
		"thing":       types.NewCounter(nowNano, 50, host, nil),
		"other:thing": types.NewCounter(nowNano, 90, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected := newFakeAggregator()
	expected.Counters["some"] = map[string]types.Counter{
		"thing":       types.NewCounter(nowNano, 0, host, nil),
		"other:thing": types.NewCounter(nowNano, 0, host, nil),
	}
	expected.now = nowFn

	assert.Equal(expected.Counters, actual.Counters)

	actual = newFakeAggregator()
	actual.Timers["some"] = map[string]types.Timer{
		"thing": types.NewTimer(nowNano, []float64{50}, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected = newFakeAggregator()
	expected.Timers["some"] = map[string]types.Timer{
		"thing": types.NewTimer(nowNano, nil, host, nil),
	}
	expected.now = nowFn

	assert.Equal(expected.Timers, actual.Timers)

	actual = newFakeAggregator()
	actual.Gauges["some"] = map[string]types.Gauge{
		"thing":       types.NewGauge(nowNano, 50, host, nil),
		"other:thing": types.NewGauge(nowNano, 90, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected = newFakeAggregator()
	expected.Gauges["some"] = map[string]types.Gauge{
		"thing":       types.NewGauge(nowNano, 50, host, nil),
		"other:thing": types.NewGauge(nowNano, 90, host, nil),
	}
	expected.now = nowFn

	assert.Equal(expected.Gauges, actual.Gauges)

	actual = newFakeAggregator()
	actual.Sets["some"] = map[string]types.Set{
		"thing": types.NewSet(nowNano, map[string]struct{}{"user": {}}, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected = newFakeAggregator()
	expected.Sets["some"] = map[string]types.Set{
		"thing": types.NewSet(nowNano, make(map[string]struct{}), host, nil),
	}
	expected.now = nowFn

	assert.Equal(expected.Sets, actual.Sets)

	// expired
	pastNano := types.Nanotime(now.Add(-30 * time.Second).UnixNano())

	actual = newFakeAggregator()
	actual.expiryInterval = 10 * time.Second
	actual.Counters["some"] = map[string]types.Counter{
		"thing":       types.NewCounter(pastNano, 50, host, nil),
		"other:thing": types.NewCounter(pastNano, 90, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected = newFakeAggregator()
	expected.now = nowFn

	assert.Equal(expected.Counters, actual.Counters)

	actual = newFakeAggregator()
	actual.expiryInterval = 10 * time.Second
	actual.Timers["some"] = map[string]types.Timer{
		"thing": types.NewTimer(pastNano, []float64{50}, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected = newFakeAggregator()
	expected.now = nowFn

	assert.Equal(expected.Timers, actual.Timers)

	actual = newFakeAggregator()
	actual.expiryInterval = 10 * time.Second
	actual.Gauges["some"] = map[string]types.Gauge{
		"thing":       types.NewGauge(pastNano, 50, host, nil),
		"other:thing": types.NewGauge(pastNano, 90, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected = newFakeAggregator()
	expected.now = nowFn

	assert.Equal(expected.Gauges, actual.Gauges)

	actual = newFakeAggregator()
	actual.expiryInterval = 10 * time.Second
	actual.Sets["some"] = map[string]types.Set{
		"thing": types.NewSet(pastNano, map[string]struct{}{"user": {}}, host, nil),
	}
	actual.now = nowFn
	actual.Reset()

	expected = newFakeAggregator()
	expected.now = nowFn

	assert.Equal(expected.Sets, actual.Sets)
}

func TestIsExpired(t *testing.T) {
	assert := assert.New(t)

	now := types.Nanotime(time.Now().UnixNano())

	ma := &aggregator{expiryInterval: 0}
	assert.Equal(false, ma.isExpired(now, now))

	ma.expiryInterval = 10 * time.Second

	ts := types.Nanotime(time.Now().Add(-30 * time.Second).UnixNano())
	assert.Equal(true, ma.isExpired(now, ts))

	ts = types.Nanotime(time.Now().Add(-1 * time.Second).UnixNano())
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
	nowNano := types.Nanotime(now.UnixNano())

	tests := metricsFixtures()
	for _, metric := range tests {
		ma.Receive(&metric, now)
	}

	expectedCounters := types.Counters{
		"foo.bar.baz": map[string]types.Counter{
			"": {Value: 2, Timestamp: nowNano},
		},
		"smp.rte": map[string]types.Counter{
			"":            {Value: 50, Timestamp: nowNano},
			"baz,foo:bar": {Value: 55, Timestamp: nowNano, Tags: types.Tags{"baz", "foo:bar"}},
		},
	}
	assert.Equal(expectedCounters, ma.Counters)

	expectedGauges := types.Gauges{
		"abc.def.g": map[string]types.Gauge{
			"":            {Value: 3, Timestamp: nowNano},
			"baz,foo:bar": {Value: 8, Timestamp: nowNano, Tags: types.Tags{"baz", "foo:bar"}},
		},
	}
	assert.Equal(expectedGauges, ma.Gauges)

	expectedTimers := types.Timers{
		"def.g": map[string]types.Timer{
			"":            {Values: []float64{10}, Timestamp: nowNano},
			"baz,foo:bar": {Values: []float64{1}, Timestamp: nowNano, Tags: types.Tags{"baz", "foo:bar"}},
		},
	}
	assert.Equal(expectedTimers, ma.Timers)

	expectedSets := types.Sets{
		"uniq.usr": map[string]types.Set{
			"": {
				Values: map[string]struct{}{
					"joe":  {},
					"bob":  {},
					"john": {},
				},
				Timestamp: nowNano,
			},
			"baz,foo:bar": {
				Values: map[string]struct{}{
					"john": {},
				},
				Timestamp: nowNano,
				Tags:      types.Tags{"baz", "foo:bar"},
			},
		},
	}
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
