package statsd

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atlassian/gostatsd"
	. "github.com/atlassian/gostatsd/internal/fixtures"
)

func TestTagStripMergesCounters(t *testing.T) {
	t.Parallel()
	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, []Filter{
		{DropTags: gostatsd.StringMatchList{gostatsd.NewStringMatch("key2:*")}},
	})

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(&gostatsd.Metric{Type: gostatsd.COUNTER, Name: "metric", Timestamp: 10, Tags: gostatsd.Tags{"key:value"}, Value: 20, Rate: 1})                              // Will merge in to metric with TS 20
	mm.Receive(&gostatsd.Metric{Type: gostatsd.COUNTER, Name: "metric", Timestamp: 20, Tags: gostatsd.Tags{"key:value", "key2:value2"}, Value: 1, Rate: 0.1})              // key2:value2 will be dropped, and TS 10 will merge in to it
	mm.Receive(&gostatsd.Metric{Type: gostatsd.COUNTER, Name: "metric", Timestamp: 30, Tags: gostatsd.Tags{"key:value", "key2:value2", "key3:value3"}, Value: 1, Rate: 1}) // key2:value2 will be dropped, but it won't merge in to anything

	expected := gostatsd.NewMetricMap(false)
	expected.Counters["metric"] = map[string]gostatsd.Counter{
		"key:value":             {Timestamp: 20, Value: 30, Tags: gostatsd.Tags{"key:value"}},
		"key3:value3,key:value": {Timestamp: 30, Value: 1, Tags: gostatsd.Tags{"key3:value3", "key:value"}},
	}

	// TagHandler.DispatchMetricMap has 2 possible executing orderings when resolving a conflicting, depending on map
	// traversal order which is random by design, so we run the core of the test a few times, validating every iteration.
	for i := 0; i < 50; i++ {
		th.DispatchMetricMap(context.Background(), mm)
		require.EqualValues(t, expected, tch.mm[0])
		tch.mm = nil
	}
}

func TestTagStripMergesGauges(t *testing.T) {
	t.Parallel()
	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, []Filter{
		{DropTags: gostatsd.StringMatchList{gostatsd.NewStringMatch("key2:*")}},
	})

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(&gostatsd.Metric{Type: gostatsd.GAUGE, Name: "metric", Timestamp: 10, Tags: gostatsd.Tags{"key:value"}, Value: 10})                               // Will merge in to metric with TS 20
	mm.Receive(&gostatsd.Metric{Type: gostatsd.GAUGE, Name: "metric", Timestamp: 20, Tags: gostatsd.Tags{"key:value", "key2:value2"}, Value: 20})                // key2:value2 will be dropped, and TS 10 will merge in to it
	mm.Receive(&gostatsd.Metric{Type: gostatsd.GAUGE, Name: "metric", Timestamp: 30, Tags: gostatsd.Tags{"key:value", "key2:value2", "key3:value3"}, Value: 30}) // key2:value2 will be dropped, but it won't merge in to anything

	expected := gostatsd.NewMetricMap(false)
	expected.Gauges["metric"] = map[string]gostatsd.Gauge{
		"key:value":             {Timestamp: 20, Value: 20, Tags: gostatsd.Tags{"key:value"}},
		"key3:value3,key:value": {Timestamp: 30, Value: 30, Tags: gostatsd.Tags{"key3:value3", "key:value"}},
	}

	// TagHandler.DispatchMetricMap has 2 possible executing orderings when resolving a conflicting, depending on map
	// traversal order which is random by design, so we run the core of the test a few times, validating every iteration.
	for i := 0; i < 50; i++ {
		th.DispatchMetricMap(context.Background(), mm)
		require.EqualValues(t, expected, tch.mm[0])
		tch.mm = nil
	}
}

func TestTagStripMergesTimers(t *testing.T) {
	t.Parallel()
	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, []Filter{
		{DropTags: gostatsd.StringMatchList{gostatsd.NewStringMatch("key2:*")}},
	})

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(&gostatsd.Metric{Type: gostatsd.TIMER, Name: "metric", Timestamp: 10, Tags: gostatsd.Tags{"key:value"}, Value: 10, Rate: 1})                               // Will merge in to metric with TS 20
	mm.Receive(&gostatsd.Metric{Type: gostatsd.TIMER, Name: "metric", Timestamp: 20, Tags: gostatsd.Tags{"key:value", "key2:value2"}, Value: 20, Rate: 1})                // key2:value2 will be dropped, and TS 10 will merge in to it
	mm.Receive(&gostatsd.Metric{Type: gostatsd.TIMER, Name: "metric", Timestamp: 30, Tags: gostatsd.Tags{"key:value", "key2:value2", "key3:value3"}, Value: 30, Rate: 1}) // key2:value2 will be dropped, but it won't merge in to anything

	expected := gostatsd.NewMetricMap(false)
	expected.Timers["metric"] = map[string]gostatsd.Timer{
		"key:value":             {Timestamp: 20, Values: []float64{10, 20}, Tags: gostatsd.Tags{"key:value"}, SampledCount: 2},
		"key3:value3,key:value": {Timestamp: 30, Values: []float64{30}, Tags: gostatsd.Tags{"key3:value3", "key:value"}, SampledCount: 1},
	}

	// TagHandler.DispatchMetricMap has 2 possible executing orderings when resolving a conflicting, depending on map
	// traversal order which is random by design, so we run the core of the test a few times, validating every iteration.
	for i := 0; i < 50; i++ {
		th.DispatchMetricMap(context.Background(), mm)
		// Make sure the actual values are deterministic
		sort.Float64s(tch.mm[0].Timers["metric"]["key:value"].Values)
		require.EqualValues(t, expected, tch.mm[0])
		tch.mm = nil
	}
}

func TestTagStripMergesSets(t *testing.T) {
	t.Parallel()
	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, []Filter{
		{DropTags: gostatsd.StringMatchList{gostatsd.NewStringMatch("key2:*")}},
	})

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(&gostatsd.Metric{Type: gostatsd.SET, Name: "metric", Timestamp: 10, Tags: gostatsd.Tags{"key:value"}, StringValue: "abc"})                               // Will merge in to metric with TS 20
	mm.Receive(&gostatsd.Metric{Type: gostatsd.SET, Name: "metric", Timestamp: 20, Tags: gostatsd.Tags{"key:value", "key2:value2"}, StringValue: "def"})                // key2:value2 will be dropped, and TS 10 will merge in to it
	mm.Receive(&gostatsd.Metric{Type: gostatsd.SET, Name: "metric", Timestamp: 30, Tags: gostatsd.Tags{"key:value", "key2:value2", "key3:value3"}, StringValue: "ghi"}) // key2:value2 will be dropped, but it won't merge in to anything

	expected := gostatsd.NewMetricMap(false)
	expected.Sets["metric"] = map[string]gostatsd.Set{
		"key:value":             {Timestamp: 20, Values: map[string]struct{}{"abc": {}, "def": {}}, Tags: gostatsd.Tags{"key:value"}},
		"key3:value3,key:value": {Timestamp: 30, Values: map[string]struct{}{"ghi": {}}, Tags: gostatsd.Tags{"key3:value3", "key:value"}},
	}

	// TagHandler.DispatchMetricMap has 2 possible executing orderings when resolving a conflicting, depending on map
	// traversal order which is random by design, so we run the core of the test a few times, validating every iteration.
	for i := 0; i < 50; i++ {
		th.DispatchMetricMap(context.Background(), mm)
		require.EqualValues(t, expected, tch.mm[0])
		tch.mm = nil
	}
}

func TestFilterPassesNoFilters(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, nil)

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(MakeMetric())
	expected := gostatsd.NewMetricMap(false)
	expected.Receive(MakeMetric())
	th.DispatchMetricMap(context.Background(), mm)
	require.Len(t, tch.mm, 1)
	require.Equal(t, expected, tch.mm[0])
}

func TestFilterPassesEmptyFilters(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, nil)
	th.filters = []Filter{}

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(MakeMetric())
	expected := gostatsd.NewMetricMap(false)
	expected.Receive(MakeMetric())
	th.DispatchMetricMap(context.Background(), mm)
	require.Len(t, tch.mm, 1)
	require.Equal(t, expected, tch.mm[0])
}

func TestFilterKeepNonMatch(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, nil)
	th.filters = []Filter{
		{
			MatchMetrics: gostatsd.StringMatchList{gostatsd.NewStringMatch("bad.name")},
			DropMetric:   true,
		},
	}

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(MakeMetric())
	expected := gostatsd.NewMetricMap(false)
	expected.Receive(MakeMetric())
	th.DispatchMetricMap(context.Background(), mm)
	require.Len(t, tch.mm, 1)
	require.Equal(t, expected, tch.mm[0])
}

func TestFilterDropsBadName(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, nil)
	th.filters = []Filter{
		{
			MatchMetrics: gostatsd.StringMatchList{gostatsd.NewStringMatch("name")},
			DropMetric:   true,
		},
	}

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(MakeMetric())
	th.DispatchMetricMap(context.Background(), mm)
	require.Len(t, tch.mm, 0) // nothing is dispatched if the entire MetricMap is dropped
}

func TestFilterDropsBadPrefix(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, nil)
	th.filters = []Filter{
		{
			MatchMetrics: gostatsd.StringMatchList{gostatsd.NewStringMatch("na*")},
			DropMetric:   true,
		},
	}

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(MakeMetric())
	th.DispatchMetricMap(context.Background(), mm)
	require.Len(t, tch.mm, 0) // nothing is dispatched if the entire MetricMap is dropped
}

func TestFilterKeepsWhitelist(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, nil)
	th.filters = []Filter{
		{
			MatchMetrics:   gostatsd.StringMatchList{gostatsd.NewStringMatch("name.*")},
			ExcludeMetrics: gostatsd.StringMatchList{gostatsd.NewStringMatch("name.good")},
			DropMetric:     true,
		},
	}

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(MakeMetric(Name("name.bad")))
	mm.Receive(MakeMetric(Name("name.good")))

	expected := gostatsd.NewMetricMap(false)
	expected.Receive(MakeMetric(Name("name.good")))

	th.DispatchMetricMap(context.Background(), mm)
	require.Len(t, tch.mm, 1)
	require.Equal(t, expected, tch.mm[0])
}

func TestFilterDropsTag(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, nil)
	th.filters = []Filter{
		{
			MatchMetrics: gostatsd.StringMatchList{gostatsd.NewStringMatch("bad.name")},
			DropTags:     gostatsd.StringMatchList{gostatsd.NewStringMatch("foo:*")},
		},
	}

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(MakeMetric(Name("bad.name")))
	expected := gostatsd.NewMetricMap(false)
	expected.Receive(MakeMetric(Name("bad.name"), DropTag("host:baz")))

	th.DispatchMetricMap(context.Background(), mm)
	require.Len(t, tch.mm, 1)
	require.Equal(t, expected, tch.mm[0])
}

func TestFilterDropsHost(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, nil)
	th.filters = []Filter{
		{
			MatchMetrics: gostatsd.StringMatchList{gostatsd.NewStringMatch("name")},
			DropHost:     true,
		},
	}

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(MakeMetric())
	expected := gostatsd.NewMetricMap(false)
	expected.Receive(MakeMetric(DropSource))

	th.DispatchMetricMap(context.Background(), mm)
	require.Len(t, tch.mm, 1)
	require.Equal(t, expected, tch.mm[0])
}

func TestNewTagHandlerFromViper(t *testing.T) {
	t.Parallel()

	var data = []byte(`
filters='drop-noisy-metric drop-noisy-metric-with-tag drop-noisy-tag drop-noisy-keep-quiet-metric drop-host'

[filter.drop-noisy-metric]
match-metrics='noisy.*'
drop-metric=true

[filter.drop-noisy-metric-with-tag]
match-metrics='noisy.*'
match-tags='noisy-tag:*'
drop-metric=true

[filter.drop-noisy-tag]
match-metrics='noisy.*'
drop-tags='noisy-tag:*'

[filter.drop-noisy-keep-quiet-metric]
match-metrics='noisy.*'
exclude-metrics='noisy.quiet.* noisy.ok.*'
drop-metric=true

[filter.drop-host]
match-metrics='global.*'
drop-host=true
drop-tags='host:*'
`)

	v := viper.New()
	v.SetConfigType("toml")
	err := v.ReadConfig(bytes.NewBuffer(data))
	assert.NoError(t, err)
	if err != nil {
		return
	}

	nh := &nopHandler{}
	th := NewTagHandlerFromViper(v, nh, nil)

	empty := gostatsd.StringMatchList{}

	expected := []Filter{
		{MatchMetrics: toStringMatch([]string{"noisy.*"}), ExcludeMetrics: empty, MatchTags: empty, DropTags: empty, DropMetric: true, DropHost: false},
		{MatchMetrics: toStringMatch([]string{"noisy.*"}), ExcludeMetrics: empty, MatchTags: toStringMatch([]string{"noisy-tag:*"}), DropTags: empty, DropMetric: true, DropHost: false},
		{MatchMetrics: toStringMatch([]string{"noisy.*"}), ExcludeMetrics: empty, MatchTags: empty, DropTags: toStringMatch([]string{"noisy-tag:*"}), DropMetric: false, DropHost: false},
		{MatchMetrics: toStringMatch([]string{"noisy.*"}), ExcludeMetrics: toStringMatch([]string{"noisy.quiet.*", "noisy.ok.*"}), DropTags: empty, MatchTags: empty, DropMetric: true, DropHost: false},
		{MatchMetrics: toStringMatch([]string{"global.*"}), ExcludeMetrics: empty, MatchTags: empty, DropTags: toStringMatch([]string{"host:*"}), DropMetric: false, DropHost: true},
	}
	assert.Equal(t, expected, th.filters)
}

func assertHasAllTags(t *testing.T, actual gostatsd.Tags, expected ...string) {
	assert.Equal(t, len(expected), len(actual))
	seenActual := map[string]struct{}{}
	for _, actualTag := range actual {
		seenActual[actualTag] = struct{}{}
	}
	assert.Equal(t, len(actual), len(seenActual), "found duplicates in actual")
	for _, expectedTag := range expected {
		if _, ok := seenActual[expectedTag]; !ok {
			assert.Fail(
				t,
				"missing tag",
				"have tags: [%s], expected tags: [%s], missing tag: %v",
				strings.Join(actual, ","),
				strings.Join(expected, ","),
				expectedTag,
			)
		}
	}

	seenExpected := map[string]struct{}{}
	for _, expectedTag := range expected {
		seenExpected[expectedTag] = struct{}{}
	}
	assert.Equal(t, len(expected), len(seenExpected), "found duplicates in expected")
	for _, actualTag := range actual {
		if _, ok := seenExpected[actualTag]; !ok {
			assert.Fail(
				t,
				"extra tag",
				"have tags: [%s], expected tags: [%s], extra tag: %s",
				strings.Join(actual, ","),
				strings.Join(expected, ","),
				actualTag,
			)
		}
	}
}

func TestTagMetricHandlerAddsNoTags(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, nil)

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(MakeMetric(DropSource))
	expected := gostatsd.NewMetricMap(false)
	expected.Receive(MakeMetric(DropSource)) // No hostname added

	th.DispatchMetricMap(context.Background(), mm)
	require.Len(t, tch.mm, 1)
	require.Equal(t, expected, tch.mm[0])
}

func TestTagMetricHandlerAddsSingleTag(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{"tag1"}, nil)

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(MakeMetric())
	expected := gostatsd.NewMetricMap(false)
	expected.Receive(MakeMetric(AddTag("tag1")))

	th.DispatchMetricMap(context.Background(), mm)
	require.Len(t, tch.mm, 1)
	require.Equal(t, expected, tch.mm[0])
}

func TestTagMetricHandlerAddsMultipleTags(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{"tag1", "tag2"}, nil)

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(MakeMetric())
	expected := gostatsd.NewMetricMap(false)
	expected.Receive(MakeMetric(AddTag("tag1", "tag2")))

	th.DispatchMetricMap(context.Background(), mm)
	require.Len(t, tch.mm, 1)
	require.Equal(t, expected, tch.mm[0])
}

func TestTagMetricHandlerAddsDuplicateTags(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{"tag1", "tag2", "tag2", "tag3", "tag1"}, nil)

	mm := gostatsd.NewMetricMap(false)
	mm.Receive(MakeMetric())
	expected := gostatsd.NewMetricMap(false)
	expected.Receive(MakeMetric(AddTag("tag1", "tag2", "tag3")))

	th.DispatchMetricMap(context.Background(), mm)
	require.Len(t, tch.mm, 1)
	require.Equal(t, expected, tch.mm[0])
}

func TestTagEventHandlerAddsNoTags(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, nil)

	e := &gostatsd.Event{}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e)) // Metric tracked
	assertHasAllTags(t, tch.e[0].Tags)
	assert.Equal(t, gostatsd.UnknownSource, tch.e[0].Source) // No hostname added
}

func TestTagEventHandlerAddsSingleTag(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{"tag1"}, nil)

	e := &gostatsd.Event{}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e)) // Metric tracked
	assertHasAllTags(t, tch.e[0].Tags, "tag1")
	assert.Equal(t, gostatsd.UnknownSource, tch.e[0].Source) // No hostname added
}

func TestTagEventHandlerAddsMultipleTags(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{"tag1", "tag2"}, nil)

	e := &gostatsd.Event{}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e)) // Metric tracked
	assertHasAllTags(t, tch.e[0].Tags, "tag1", "tag2")
	assert.Equal(t, gostatsd.UnknownSource, tch.e[0].Source) // No hostname added
}

func TestTagEventHandlerAddsHostname(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{}, nil)

	e := &gostatsd.Event{
		Source: "1.2.3.4",
	}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e)) // Metric tracked
	assertHasAllTags(t, tch.e[0].Tags)
	assert.Equal(t, gostatsd.Source("1.2.3.4"), tch.e[0].Source) // Hostname injected
}

func TestTagEventHandlerAddsDuplicateTags(t *testing.T) {
	t.Parallel()

	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{"tag1", "tag2", "tag2", "tag3", "tag1"}, nil)

	e := &gostatsd.Event{}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e)) // Metric tracked
	assertHasAllTags(t, tch.e[0].Tags, "tag1", "tag2", "tag3")
	assert.Equal(t, gostatsd.UnknownSource, tch.e[0].Source) // No hostname added
}

func BenchmarkTagMetricHandlerAddsDuplicateTagsSmall(b *testing.B) {
	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"cccccccccccccccccccccccccccccccc:cccccccccccccccccccccccccccccccc",
	}, nil)

	b.ReportAllocs()
	b.ResetTimer()

	baseTags := gostatsd.Tags{
		"cccccccccccccccccccccccccccccccc:cccccccccccccccccccccccccccccccc",
		"dddddddddddddddddddddddddddddddd:dddddddddddddddddddddddddddddddd",
		"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
	}

	for n := 0; n < b.N; n++ {
		metricTags := make(gostatsd.Tags, 0, len(baseTags)+th.EstimatedTags())
		metricTags = append(metricTags, baseTags...)
		m := &gostatsd.Metric{
			Type: gostatsd.COUNTER,
			Tags: metricTags,
		}
		mm := gostatsd.NewMetricMap(false)
		mm.Receive(m)
		th.DispatchMetricMap(context.Background(), mm)
	}
}

func BenchmarkTagMetricHandlerAddsDuplicateTagsLarge(b *testing.B) {
	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"cccccccccccccccccccccccccccccccc:cccccccccccccccccccccccccccccccc",
		"dddddddddddddddddddddddddddddddd:dddddddddddddddddddddddddddddddd",
		"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		"ffffffffffffffffffffffffffffffff:ffffffffffffffffffffffffffffffff",
		"gggggggggggggggggggggggggggggggg:gggggggggggggggggggggggggggggggg",
		"hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh:hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh",
		"iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii:iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii",
		"jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj:jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj",
	}, nil)

	b.ReportAllocs()
	b.ResetTimer()

	baseTags := gostatsd.Tags{
		"hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh:hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh",
		"iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii:iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii",
		"jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj:jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj",
		"kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk:kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk",
		"llllllllllllllllllllllllllllllll:llllllllllllllllllllllllllllllll",
	}

	for n := 0; n < b.N; n++ {
		metricTags := make(gostatsd.Tags, 0, len(baseTags)+th.EstimatedTags())
		metricTags = append(metricTags, baseTags...)
		m := &gostatsd.Metric{
			Type: gostatsd.COUNTER,
			Tags: metricTags,
		}
		mm := gostatsd.NewMetricMap(false)
		mm.Receive(m)
		th.DispatchMetricMap(context.Background(), mm)
	}
}

func BenchmarkTagEventHandlerAddsDuplicateTagsSmall(b *testing.B) {
	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"cccccccccccccccccccccccccccccccc:cccccccccccccccccccccccccccccccc",
	}, nil)

	eventTags := gostatsd.Tags{
		"cccccccccccccccccccccccccccccccc:cccccccccccccccccccccccccccccccc",
		"dddddddddddddddddddddddddddddddd:dddddddddddddddddddddddddddddddd",
		"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		e := &gostatsd.Event{
			Tags: eventTags.Copy(),
		}
		th.DispatchEvent(context.Background(), e)
	}
}

func BenchmarkTagEventHandlerAddsDuplicateTagsLarge(b *testing.B) {
	tch := &capturingHandler{}
	th := NewTagHandler(tch, gostatsd.Tags{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"cccccccccccccccccccccccccccccccc:cccccccccccccccccccccccccccccccc",
		"dddddddddddddddddddddddddddddddd:dddddddddddddddddddddddddddddddd",
		"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		"ffffffffffffffffffffffffffffffff:ffffffffffffffffffffffffffffffff",
		"gggggggggggggggggggggggggggggggg:gggggggggggggggggggggggggggggggg",
		"hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh:hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh",
		"iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii:iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii",
		"jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj:jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj",
	}, nil)

	eventTags := gostatsd.Tags{
		"hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh:hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh",
		"iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii:iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii",
		"jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj:jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj",
		"kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk:kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk",
		"llllllllllllllllllllllllllllllll:llllllllllllllllllllllllllllllll",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		e := &gostatsd.Event{
			Tags: eventTags.Copy(),
		}
		th.DispatchEvent(context.Background(), e)
	}
}
