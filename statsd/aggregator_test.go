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
	)
}

func TestNewMetricAggregator(t *testing.T) {
	assert := assert.New(t)

	b, actual := newFakeMetricAggregator()

	if assert.NotNil(actual.Senders) {
		assert.Equal(b, actual.Senders[0])
	}

	if assert.NotNil(actual.Counters) {
		assert.Equal(make(map[string]map[string]types.Counter), actual.Counters)
	}

	if assert.NotNil(actual.Timers) {
		assert.Equal(make(map[string]map[string]types.Timer), actual.Timers)
	}

	if assert.NotNil(actual.Gauges) {
		assert.Equal(make(map[string]map[string]types.Gauge), actual.Gauges)
	}

	if assert.NotNil(actual.Sets) {
		assert.Equal(make(map[string]map[string]types.Set), actual.Sets)
	}

	if assert.NotNil(actual.MetricChan) {
		assert.Equal("chan types.Metric", reflect.TypeOf(actual.MetricChan).String())
	}
}

func TestFlush(t *testing.T) {
	//assert := assert.New(t)

}

func TestReset(t *testing.T) {
	//assert := assert.New(t)

}

func TestReceiveMetric(t *testing.T) {
	assert := assert.New(t)

	_, ma := newFakeMetricAggregator()

	tests := []types.Metric{
		{Name: "foo.bar.baz", Value: 2, Type: types.COUNTER},
		{Name: "abc.def.g", Value: 3, Type: types.GAUGE},
		{Name: "abc.def.g", Value: 8, Type: types.GAUGE, Tags: types.Tags{Items: []string{"foo:bar", "baz"}}},
		{Name: "def.g", Value: 10, Type: types.TIMER},
		{Name: "def.g", Value: 1, Type: types.TIMER, Tags: types.Tags{Items: []string{"foo:bar", "baz"}}},
		{Name: "smp.rte", Value: 50, Type: types.COUNTER},
		{Name: "smp.rte", Value: 50, Type: types.COUNTER, Tags: types.Tags{Items: []string{"foo:bar", "baz"}}},
		{Name: "smp.rte", Value: 5, Type: types.COUNTER, Tags: types.Tags{Items: []string{"foo:bar", "baz"}}},
		{Name: "uniq.usr", StringValue: "joe", Type: types.SET},
		{Name: "uniq.usr", StringValue: "joe", Type: types.SET},
		{Name: "uniq.usr", StringValue: "bob", Type: types.SET},
		{Name: "uniq.usr", StringValue: "john", Type: types.SET},
		{Name: "uniq.usr", StringValue: "john", Type: types.SET, Tags: types.Tags{Items: []string{"foo:bar", "baz"}}},
	}

	for _, metric := range tests {
		ma.receiveMetric(metric)
	}

	expectedCounters := make(map[string]map[string]types.Counter)
	expectedCounters["foo.bar.baz"] = make(map[string]types.Counter)
	expectedCounters["foo.bar.baz"][""] = types.Counter{Value: 2}
	expectedCounters["smp.rte"] = make(map[string]types.Counter)
	expectedCounters["smp.rte"]["baz,foo:bar"] = types.Counter{Value: 55}
	expectedCounters["smp.rte"][""] = types.Counter{Value: 50}
	assert.Equal(expectedCounters, ma.Counters)

	expectedGauges := make(map[string]map[string]types.Gauge)
	expectedGauges["abc.def.g"] = make(map[string]types.Gauge)
	expectedGauges["abc.def.g"][""] = types.Gauge{Value: 3}
	expectedGauges["abc.def.g"]["baz,foo:bar"] = types.Gauge{Value: 8}
	assert.Equal(expectedGauges, ma.Gauges)

	expectedTimers := make(map[string]map[string]types.Timer)
	expectedTimers["def.g"] = make(map[string]types.Timer)
	expectedTimers["def.g"][""] = types.Timer{Values: []float64{10}}
	expectedTimers["def.g"]["baz,foo:bar"] = types.Timer{Values: []float64{1}}
	assert.Equal(expectedTimers, ma.Timers)

	expectedSets := make(map[string]map[string]types.Set)
	expectedSets["uniq.usr"] = make(map[string]types.Set)
	sets := make(map[string]int64)
	sets["joe"] = 2
	sets["bob"] = 1
	sets["john"] = 1
	sets2 := make(map[string]int64)
	sets2["john"] = 1
	expectedSets["uniq.usr"][""] = types.Set{Values: sets}
	expectedSets["uniq.usr"]["baz,foo:bar"] = types.Set{Values: sets2}
	assert.Equal(expectedSets, ma.Sets)
}
