package statsd

import (
	"reflect"
	"testing"
	"time"

	"github.com/jtblin/gostatsd/backend"
	"github.com/jtblin/gostatsd/types"

	"github.com/stretchr/testify/assert"
)

type FakeBackend struct{}

func (fb *FakeBackend) SendMetrics(metrics types.MetricMap) error { return nil }
func (fb *FakeBackend) SampleConfig() string                      { return "" }
func (fb *FakeBackend) Name() string                              { return "fake" }

func newFakeMetricAggregator() (backend.MetricSender, MetricAggregator) {
	b := &FakeBackend{}
	var backends []backend.MetricSender
	backends = append(backends, b)
	return b, NewMetricAggregator(
		backends,
		[]float64{float64(90)},
		time.Duration(10)*time.Second,
		time.Duration(5)*time.Minute,
	)
}

func TestNewMetricAggregator(t *testing.T) {
	assert := assert.New(t)

	b, actual := newFakeMetricAggregator()

	if assert.NotNil(actual.Senders) {
		assert.Equal(b, actual.Senders[0])
	}

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

	if assert.NotNil(actual.MetricChan) {
		assert.Equal("chan types.Metric", reflect.TypeOf(actual.MetricChan).String())
	}
}

func TestFlush(t *testing.T) {
	//assert := assert.New(t)

}

func TestReset(t *testing.T) {
	assert := assert.New(t)
	now := time.Now()

	// non expired
	_, actual := newFakeMetricAggregator()
	actual.Counters["some"] = make(map[string]types.Counter)
	actual.Counters["some"]["thing"] = types.NewCounter(now, time.Duration(10)*time.Second, int64(50))
	actual.Counters["some"]["other:thing"] = types.NewCounter(now, time.Duration(10)*time.Second, int64(90))
	actual.Reset(now)

	_, expected := newFakeMetricAggregator()
	expected.Counters["some"] = make(map[string]types.Counter)
	expected.Counters["some"]["thing"] = types.NewCounter(now, time.Duration(10)*time.Second, int64(0))
	expected.Counters["some"]["other:thing"] = types.NewCounter(now, time.Duration(10)*time.Second, int64(0))

	assert.Equal(expected.Counters, actual.Counters)

	_, actual = newFakeMetricAggregator()
	actual.Timers["some"] = make(map[string]types.Timer)
	actual.Timers["some"]["thing"] = types.NewTimer(now, time.Duration(10)*time.Second, []float64{50})
	actual.Reset(now)

	_, expected = newFakeMetricAggregator()
	expected.Timers["some"] = make(map[string]types.Timer)
	expected.Timers["some"]["thing"] = types.Timer{Interval: types.Interval{Timestamp: now, Flush: time.Duration(10) * time.Second}}

	assert.Equal(expected.Timers, actual.Timers)

	_, actual = newFakeMetricAggregator()
	actual.Gauges["some"] = make(map[string]types.Gauge)
	actual.Gauges["some"]["thing"] = types.NewGauge(now, time.Duration(10)*time.Second, float64(50))
	actual.Gauges["some"]["other:thing"] = types.NewGauge(now, time.Duration(10)*time.Second, float64(90))
	actual.Reset(now)

	_, expected = newFakeMetricAggregator()
	expected.Gauges["some"] = make(map[string]types.Gauge)
	expected.Gauges["some"]["thing"] = types.NewGauge(now, time.Duration(10)*time.Second, float64(50))
	expected.Gauges["some"]["other:thing"] = types.NewGauge(now, time.Duration(10)*time.Second, float64(90))

	assert.Equal(expected.Gauges, actual.Gauges)

	_, actual = newFakeMetricAggregator()
	actual.Sets["some"] = make(map[string]types.Set)
	unique := make(map[string]int64)
	unique["user"] = 1
	actual.Sets["some"]["thing"] = types.NewSet(now, time.Duration(10)*time.Second, unique)
	actual.Reset(now)

	_, expected = newFakeMetricAggregator()
	expected.Sets["some"] = make(map[string]types.Set)
	expected.Sets["some"]["thing"] = types.NewSet(now, time.Duration(10)*time.Second, make(map[string]int64))

	assert.Equal(expected.Sets, actual.Sets)

	// expired
	past := now.Add(-30 * time.Second)

	_, actual = newFakeMetricAggregator()
	actual.ExpiryInterval = time.Duration(10) * time.Second
	actual.Counters["some"] = make(map[string]types.Counter)
	actual.Counters["some"]["thing"] = types.NewCounter(past, time.Duration(10)*time.Second, int64(50))
	actual.Counters["some"]["other:thing"] = types.NewCounter(past, time.Duration(10)*time.Second, int64(90))
	actual.Reset(now)

	_, expected = newFakeMetricAggregator()

	assert.Equal(expected.Counters, actual.Counters)

	_, actual = newFakeMetricAggregator()
	actual.ExpiryInterval = time.Duration(10) * time.Second
	actual.Timers["some"] = make(map[string]types.Timer)
	actual.Timers["some"]["thing"] = types.NewTimer(past, time.Duration(10)*time.Second, []float64{50})
	actual.Reset(now)

	_, expected = newFakeMetricAggregator()

	assert.Equal(expected.Timers, actual.Timers)

	_, actual = newFakeMetricAggregator()
	actual.ExpiryInterval = time.Duration(10) * time.Second
	actual.Gauges["some"] = make(map[string]types.Gauge)
	actual.Gauges["some"]["thing"] = types.NewGauge(past, time.Duration(10)*time.Second, float64(50))
	actual.Gauges["some"]["other:thing"] = types.NewGauge(past, time.Duration(10)*time.Second, float64(90))
	actual.Reset(now)

	_, expected = newFakeMetricAggregator()

	assert.Equal(expected.Gauges, actual.Gauges)

	_, actual = newFakeMetricAggregator()
	actual.ExpiryInterval = time.Duration(10) * time.Second
	actual.Sets["some"] = make(map[string]types.Set)
	unique = make(map[string]int64)
	unique["user"] = 1
	actual.Sets["some"]["thing"] = types.NewSet(past, time.Duration(10)*time.Second, unique)
	actual.Reset(now)

	_, expected = newFakeMetricAggregator()

	assert.Equal(expected.Sets, actual.Sets)
}

func TestIsExpired(t *testing.T) {
	assert := assert.New(t)

	now := time.Now()

	ma := &MetricAggregator{ExpiryInterval: time.Duration(0)}
	assert.Equal(false, ma.isExpired(now, now))

	ma.ExpiryInterval = time.Duration(10) * time.Second

	ts := time.Now().Add(-30 * time.Second)
	assert.Equal(true, ma.isExpired(now, ts))

	ts = time.Now().Add(-1 * time.Second)
	assert.Equal(false, ma.isExpired(now, ts))
}

func TestReceiveMetric(t *testing.T) {
	assert := assert.New(t)

	_, ma := newFakeMetricAggregator()
	now := time.Now()
	d := time.Duration(10) * time.Second
	interval := types.Interval{Timestamp: now, Flush: d}

	tests := []types.Metric{
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

	for _, metric := range tests {
		ma.receiveMetric(metric, now)
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
