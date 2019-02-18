package gostatsd

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func metricsFixtures() []*Metric {
	ms := []*Metric{
		{Name: "foo.bar.baz", Value: 2, Type: COUNTER},
		{Name: "abc.def.g", Value: 3, Type: GAUGE},
		{Name: "abc.def.g", Value: 8, Type: GAUGE, Tags: Tags{"foo:bar", "baz"}},
		{Name: "def.g", Value: 10, Type: TIMER},
		{Name: "def.g", Value: 1, Type: TIMER, Tags: Tags{"foo:bar", "baz"}},
		{Name: "smp.rte", Value: 50, Type: COUNTER},
		{Name: "smp.rte", Value: 50, Type: COUNTER, Tags: Tags{"foo:bar", "baz"}},
		{Name: "smp.rte", Value: 5, Type: COUNTER, Tags: Tags{"foo:bar", "baz"}},
		{Name: "uniq.usr", StringValue: "joe", Type: SET},
		{Name: "uniq.usr", StringValue: "joe", Type: SET},
		{Name: "uniq.usr", StringValue: "bob", Type: SET},
		{Name: "uniq.usr", StringValue: "john", Type: SET},
		{Name: "uniq.usr", StringValue: "john", Type: SET, Tags: Tags{"foo:bar", "baz"}},
		{Name: "timer_sampling", Value: 10, Type: TIMER, Rate: 0.1},
		{Name: "timer_sampling", Value: 30, Type: TIMER, Rate: 0.1},
		{Name: "timer_sampling", Value: 50, Type: TIMER, Rate: 0.1},
		{Name: "counter_sampling", Value: 2, Type: COUNTER, Rate: 0.25},
		{Name: "counter_sampling", Value: 5, Type: COUNTER, Rate: 0.25},
	}
	for i, m := range ms {
		if ms[i].Rate == 0.0 {
			ms[i].Rate = 1.0
		}
		ms[i].TagsKey = m.FormatTagsKey()
	}
	return ms
}

func TestReceive(t *testing.T) {
	t.Parallel()
	assrt := assert.New(t)

	mm := NewMetricMap()
	now := time.Now()
	nowNano := Nanotime(now.UnixNano())

	tests := metricsFixtures()
	for _, metric := range tests {
		mm.Receive(metric, now)
	}

	expectedCounters := Counters{
		"foo.bar.baz": map[string]Counter{
			"": {Value: 2, Timestamp: nowNano},
		},
		"smp.rte": map[string]Counter{
			"":            {Value: 50, Timestamp: nowNano},
			"baz,foo:bar": {Value: 55, Timestamp: nowNano, Tags: Tags{"baz", "foo:bar"}},
		},
		"counter_sampling": map[string]Counter{
			"": {Value: 28, Timestamp: nowNano},
		},
	}
	assrt.Equal(expectedCounters, mm.Counters)

	expectedGauges := Gauges{
		"abc.def.g": map[string]Gauge{
			"":            {Value: 3, Timestamp: nowNano},
			"baz,foo:bar": {Value: 8, Timestamp: nowNano, Tags: Tags{"baz", "foo:bar"}},
		},
	}
	assrt.Equal(expectedGauges, mm.Gauges)

	expectedTimers := Timers{
		"def.g": map[string]Timer{
			"":            {Values: []float64{10}, Timestamp: nowNano, SampledCount: 1},
			"baz,foo:bar": {Values: []float64{1}, Timestamp: nowNano, SampledCount: 1, Tags: Tags{"baz", "foo:bar"}},
		},
		"timer_sampling": map[string]Timer{
			"": {Values: []float64{10, 30, 50}, Timestamp: nowNano, SampledCount: 30},
		},
	}
	assrt.Equal(expectedTimers, mm.Timers)

	expectedSets := Sets{
		"uniq.usr": map[string]Set{
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
				Tags:      Tags{"baz", "foo:bar"},
			},
		},
	}
	assrt.Equal(expectedSets, mm.Sets)
}

func benchmarkReceive(metric Metric, b *testing.B) {
	ma := NewMetricMap()
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ma.Receive(&metric, now)
	}
}

func BenchmarkReceiveCounter(b *testing.B) {
	benchmarkReceive(Metric{Name: "foo.bar.baz", Value: 2, Type: COUNTER}, b)
}

func BenchmarkReceiveGauge(b *testing.B) {
	benchmarkReceive(Metric{Name: "abc.def.g", Value: 3, Type: GAUGE}, b)
}

func BenchmarkReceiveTimer(b *testing.B) {
	benchmarkReceive(Metric{Name: "def.g", Value: 10, Type: TIMER}, b)
}

func BenchmarkReceiveSet(b *testing.B) {
	benchmarkReceive(Metric{Name: "uniq.usr", StringValue: "joe", Type: SET}, b)
}

func BenchmarkReceives(b *testing.B) {
	ma := NewMetricMap()
	now := time.Now()
	tests := metricsFixtures()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, metric := range tests {
			ma.Receive(metric, now)
		}
	}
}

func TestMetricMapDispatch(t *testing.T) {
	ctx, done := testContext(t)
	defer done()

	mm := NewMetricMap()
	metrics := metricsFixtures()
	for _, metric := range metrics {
		mm.Receive(metric, time.Unix(0, 0))
	}
	ch := &capturingHandler{}

	mm.DispatchMetrics(ctx, ch)

	expected := []*Metric{
		{Name: "abc.def.g", Value: 3, Rate: 1, Type: GAUGE},
		{Name: "abc.def.g", Value: 8, Rate: 1, TagsKey: "baz,foo:bar", Tags: Tags{"baz", "foo:bar"}, Type: GAUGE},
		{Name: "counter_sampling", Value: (2 + 5) / 0.25, Rate: 1, Type: COUNTER},
		{Name: "def.g", Value: 10, Rate: 1, Type: TIMER},
		{Name: "def.g", Value: 1, Rate: 1, TagsKey: "baz,foo:bar", Tags: Tags{"baz", "foo:bar"}, Type: TIMER},
		{Name: "foo.bar.baz", Value: 2, Rate: 1, Type: COUNTER},
		{Name: "smp.rte", Value: 50, Rate: 1, Type: COUNTER},
		{Name: "smp.rte", Value: 50 + 5, Rate: 1, TagsKey: "baz,foo:bar", Tags: Tags{"baz", "foo:bar"}, Type: COUNTER},
		{Name: "timer_sampling", Value: 10, Rate: 0.1, Type: TIMER},
		{Name: "timer_sampling", Value: 30, Rate: 0.1, Type: TIMER},
		{Name: "timer_sampling", Value: 50, Rate: 0.1, Type: TIMER},
		{Name: "uniq.usr", StringValue: "bob", Rate: 1, Type: SET},
		{Name: "uniq.usr", StringValue: "joe", Rate: 1, Type: SET},
		{Name: "uniq.usr", StringValue: "john", Rate: 1, Type: SET},
		{Name: "uniq.usr", StringValue: "john", Rate: 1, TagsKey: "baz,foo:bar", Tags: Tags{"baz", "foo:bar"}, Type: SET},
	}

	cmpSort := func(slice []*Metric) func(i, j int) bool {
		return func(i, j int) bool {
			if slice[i].Name == slice[j].Name {
				if len(slice[i].Tags) == len(slice[j].Tags) { // This is not exactly accurate, but close enough with our data
					return slice[i].StringValue < slice[j].StringValue
				}
				return len(slice[i].Tags) < len(slice[j].Tags)
			}
			return slice[i].Name < slice[j].Name
		}
	}

	actual := ch.GetMetrics()

	sort.Slice(actual, cmpSort(actual))
	sort.Slice(expected, cmpSort(expected))

	require.EqualValues(t, expected, actual)
}

func TestMetricMapMerge(t *testing.T) {
	metrics1 := []*Metric{
		{Name: "TestMetricMapMerge.counter", Value: 10, Rate: 1, Type: COUNTER},
		{Name: "TestMetricMapMerge.gauge", Value: 10, Type: GAUGE},
		{Name: "TestMetricMapMerge.timer", Value: 10, Rate: 1, Type: TIMER},
		{Name: "TestMetricMapMerge.timer", Value: 10, Rate: 0.1, Type: TIMER},
		{Name: "TestMetricMapMerge.set", StringValue: "abc", Type: SET},
	}
	metrics2 := []*Metric{
		{Name: "TestMetricMapMerge.counter", Value: 20, Rate: 0.1, Type: COUNTER},
		{Name: "TestMetricMapMerge.gauge", Value: 20, Type: GAUGE},
		{Name: "TestMetricMapMerge.timer", Value: 20, Rate: 1, Type: TIMER},
		{Name: "TestMetricMapMerge.timer", Value: 20, Rate: 0.1, Type: TIMER},
		{Name: "TestMetricMapMerge.set", StringValue: "def", Type: SET},
	}

	now := time.Unix(0, 0)
	m1 := NewMetricMap()
	for _, m := range metrics1 {
		m1.Receive(m, now)
	}

	later := time.Unix(1, 0)
	m2 := NewMetricMap()
	for _, m := range metrics2 {
		m2.Receive(m, later)
	}

	merged := NewMetricMap()
	merged.Merge(m1)
	merged.Merge(m2)

	expected := NewMetricMap()
	expected.Counters = Counters{
		"TestMetricMapMerge.counter": map[string]Counter{
			"": {
				Value:     10 + (20 / 0.1),
				Timestamp: Nanotime(later.UnixNano()),
			},
		},
	}
	expected.Timers = Timers{
		"TestMetricMapMerge.timer": map[string]Timer{
			"": {
				SampledCount: 1 + (1 / 0.1) + 1 + (1 / 0.1),
				Values:       []float64{10, 10, 20, 20},
				Timestamp:    Nanotime(later.UnixNano()),
			},
		},
	}
	expected.Gauges = Gauges{
		"TestMetricMapMerge.gauge": map[string]Gauge{
			"": {
				Value:     20, // most recent value wins
				Timestamp: Nanotime(later.UnixNano()),
			},
		},
	}
	expected.Sets = Sets{
		"TestMetricMapMerge.set": map[string]Set{
			"": {
				Values:    map[string]struct{}{"abc": {}, "def": {}},
				Timestamp: Nanotime(later.UnixNano()),
			},
		},
	}
	require.Equal(t, expected.Counters, merged.Counters)
	require.Equal(t, expected.Timers, merged.Timers)
	require.Equal(t, expected.Gauges, merged.Gauges)
	require.Equal(t, expected.Sets, merged.Sets)
}
