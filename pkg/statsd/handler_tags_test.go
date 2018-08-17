package statsd

import (
	"bytes"
	"context"
	"testing"

	"github.com/atlassian/gostatsd"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestFilterPassesNoFilters(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{}, nil)
	m := &gostatsd.Metric{
		Name: "name",
		Tags: gostatsd.Tags{
			"foo:bar",
			"host:baz",
		},
		Hostname: "baz",
	}
	expected := []*gostatsd.Metric{
		{
			Name: "name",
			Tags: gostatsd.Tags{
				"foo:bar",
				"host:baz",
			},
			Hostname: "baz",
		},
	}
	th.DispatchMetric(context.Background(), m)
	assert.Equal(t, expected, tch.m)
}

func TestFilterPassesEmptyFilters(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{}, nil)
	th.filters = []Filter{}
	m := &gostatsd.Metric{
		Name: "name",
		Tags: gostatsd.Tags{
			"foo:bar",
			"host:baz",
		},
		Hostname: "baz",
	}
	expected := []*gostatsd.Metric{
		{
			Name: "name",
			Tags: gostatsd.Tags{
				"foo:bar",
				"host:baz",
			},
			Hostname: "baz",
		},
	}
	th.DispatchMetric(context.Background(), m)
	assert.Equal(t, expected, tch.m)
}

func TestFilterKeepNonMatch(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{}, nil)
	th.filters = []Filter{
		{
			MatchMetrics: gostatsd.StringMatchList{gostatsd.NewStringMatch("bad.name")},
			DropMetric:   true,
		},
	}
	m := &gostatsd.Metric{
		Name: "good.name",
		Tags: gostatsd.Tags{
			"foo:bar",
			"host:baz",
		},
		Hostname: "baz",
	}
	th.DispatchMetric(context.Background(), m)
	expected := []*gostatsd.Metric{
		{
			Name: "good.name",
			Tags: gostatsd.Tags{
				"foo:bar",
				"host:baz",
			},
			Hostname: "baz",
		},
	}
	assert.Equal(t, expected, tch.m)
}

func TestFilterDropsBadName(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{}, nil)
	th.filters = []Filter{
		{
			MatchMetrics: gostatsd.StringMatchList{gostatsd.NewStringMatch("bad.name")},
			DropMetric:   true,
		},
	}
	m := &gostatsd.Metric{
		Name: "bad.name",
		Tags: gostatsd.Tags{
			"foo:bar",
			"host:baz",
		},
		Hostname: "baz",
	}
	th.DispatchMetric(context.Background(), m)
	assert.Equal(t, 0, len(tch.m))
}

func TestFilterDropsBadPrefix(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{}, nil)
	th.filters = []Filter{
		{
			MatchMetrics: gostatsd.StringMatchList{gostatsd.NewStringMatch("bad.*")},
			DropMetric:   true,
		},
	}
	m := &gostatsd.Metric{
		Name: "bad.name",
		Tags: gostatsd.Tags{
			"foo:bar",
			"host:baz",
		},
		Hostname: "baz",
	}
	th.DispatchMetric(context.Background(), m)
	assert.Equal(t, 0, len(tch.m))
}

func TestFilterKeepsWhitelist(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{}, nil)
	th.filters = []Filter{
		{
			MatchMetrics:   gostatsd.StringMatchList{gostatsd.NewStringMatch("bad.*")},
			ExcludeMetrics: gostatsd.StringMatchList{gostatsd.NewStringMatch("bad.good")},
			DropMetric:     true,
		},
	}

	m := &gostatsd.Metric{
		Name: "bad.name",
		Tags: gostatsd.Tags{
			"foo:bar",
			"host:baz",
		},
		Hostname: "baz",
	}
	th.DispatchMetric(context.Background(), m)

	m = &gostatsd.Metric{
		Name: "bad.good",
		Tags: gostatsd.Tags{
			"foo:bar",
			"host:baz",
		},
		Hostname: "baz",
	}
	th.DispatchMetric(context.Background(), m)

	expected := []*gostatsd.Metric{
		{
			Name: "bad.good",
			Tags: gostatsd.Tags{
				"foo:bar",
				"host:baz",
			},
			Hostname: "baz",
		},
	}
	assert.Equal(t, expected, tch.m)
}

func TestFilterDropsTag(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{}, nil)
	th.filters = []Filter{
		{
			MatchMetrics: gostatsd.StringMatchList{gostatsd.NewStringMatch("bad.name")},
			DropTags:     gostatsd.StringMatchList{gostatsd.NewStringMatch("foo:*")},
		},
	}

	m := &gostatsd.Metric{
		Name: "bad.name",
		Tags: gostatsd.Tags{
			"foo:bar",
			"host:baz",
		},
		Hostname: "baz",
	}
	th.DispatchMetric(context.Background(), m)

	expected := []*gostatsd.Metric{
		{
			Name: "bad.name",
			Tags: gostatsd.Tags{
				"host:baz",
			},
			Hostname: "baz",
		},
	}
	assert.Equal(t, expected, tch.m)
}

func TestFilterDropsHost(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{}, nil)
	th.filters = []Filter{
		{
			MatchMetrics: gostatsd.StringMatchList{gostatsd.NewStringMatch("bad.name")},
			DropHost:     true,
		},
	}

	m := &gostatsd.Metric{
		Name: "bad.name",
		Tags: gostatsd.Tags{
			"foo:bar",
			"host:baz",
		},
		Hostname: "baz",
	}
	th.DispatchMetric(context.Background(), m)

	expected := []*gostatsd.Metric{
		{
			Name: "bad.name",
			Tags: gostatsd.Tags{
				"foo:bar",
				"host:baz",
			},
			Hostname: "",
		},
	}
	assert.Equal(t, expected, tch.m)
}

func TestNewTagHandlerFromViper(t *testing.T) {
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
	th := NewTagHandlerFromViper(v, nh, nh, nil)

	expected := []Filter{
		{MatchMetrics: toStringMatch([]string{"noisy.*"}), DropMetric: true, DropHost: false},
		{MatchMetrics: toStringMatch([]string{"noisy.*"}), MatchTags: toStringMatch([]string{"noisy-tag:*"}), DropMetric: true, DropHost: false},
		{MatchMetrics: toStringMatch([]string{"noisy.*"}), DropTags: toStringMatch([]string{"noisy-tag:*"}), DropMetric: false, DropHost: false},
		{MatchMetrics: toStringMatch([]string{"noisy.*"}), ExcludeMetrics: toStringMatch([]string{"noisy.quiet.*", "noisy.ok.*"}), DropMetric: true, DropHost: false},
		{MatchMetrics: toStringMatch([]string{"global.*"}), DropTags: toStringMatch([]string{"host:*"}), DropMetric: false, DropHost: true},
	}
	assert.Equal(t, expected, th.filters)

}

func TestTagMetricHandlerAddsNoTags(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{}, nil)
	m := &gostatsd.Metric{}
	th.DispatchMetric(context.Background(), m)
	assert.Equal(t, 1, len(tch.m))         // Metric tracked
	assert.Equal(t, 0, len(tch.m[0].Tags)) // No tags added
	assert.Equal(t, "", tch.m[0].Hostname) // No hostname added
}

func TestTagMetricHandlerAddsSingleTag(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{"tag1"}, nil)
	m := &gostatsd.Metric{}
	th.DispatchMetric(context.Background(), m)
	assert.Equal(t, 1, len(tch.m))            // Metric tracked
	assert.Equal(t, 1, len(tch.m[0].Tags))    // 1 tag added
	assert.Equal(t, "tag1", tch.m[0].Tags[0]) //  "tag1" added
	assert.Equal(t, "", tch.m[0].Hostname)    // No hostname added
}

func TestTagMetricHandlerAddsMultipleTags(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{"tag1", "tag2"}, nil)
	m := &gostatsd.Metric{}
	th.DispatchMetric(context.Background(), m)
	assert.Equal(t, 1, len(tch.m))            // Metric tracked
	assert.Equal(t, 2, len(tch.m[0].Tags))    // 2 tag added
	assert.Equal(t, "tag1", tch.m[0].Tags[0]) //  "tag1" added
	assert.Equal(t, "tag2", tch.m[0].Tags[1]) //  "tag2" added
	assert.Equal(t, "", tch.m[0].Hostname)    // No hostname added
}

func TestTagMetricHandlerAddsHostname(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{}, nil)
	m := &gostatsd.Metric{
		SourceIP: "1.2.3.4",
	}
	th.DispatchMetric(context.Background(), m)
	assert.Equal(t, 1, len(tch.m))                // Metric tracked
	assert.Equal(t, 0, len(tch.m[0].Tags))        // No tags added
	assert.Equal(t, "1.2.3.4", tch.m[0].Hostname) // Hostname injected
}

func TestTagMetricHandlerAddsDuplicateTags(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{"tag1", "tag2", "tag2", "tag3", "tag1"}, nil)
	m := &gostatsd.Metric{}
	th.DispatchMetric(context.Background(), m)
	assert.Equal(t, 1, len(tch.m))            // Metric tracked
	assert.Equal(t, 3, len(tch.m[0].Tags))    // 3 tags added
	assert.Equal(t, "tag1", tch.m[0].Tags[0]) //  "tag1" added
	assert.Equal(t, "tag2", tch.m[0].Tags[1]) //  "tag2" added
	assert.Equal(t, "tag3", tch.m[0].Tags[2]) //  "tag3" added
	assert.Equal(t, "", tch.m[0].Hostname)    // No hostname added
}

func TestTagEventHandlerAddsNoTags(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{}, nil)
	e := &gostatsd.Event{}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e))         // Metric tracked
	assert.Equal(t, 0, len(tch.e[0].Tags)) // No tags added
	assert.Equal(t, "", tch.e[0].Hostname) // No hostname added
}

func TestTagEventHandlerAddsSingleTag(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{"tag1"}, nil)
	e := &gostatsd.Event{}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e))            // Metric tracked
	assert.Equal(t, 1, len(tch.e[0].Tags))    // 1 tag added
	assert.Equal(t, "tag1", tch.e[0].Tags[0]) //  "tag1" added
	assert.Equal(t, "", tch.e[0].Hostname)    // No hostname added
}

func TestTagEventHandlerAddsMultipleTags(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{"tag1", "tag2"}, nil)
	e := &gostatsd.Event{}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e))            // Metric tracked
	assert.Equal(t, 2, len(tch.e[0].Tags))    // 2 tag added
	assert.Equal(t, "tag1", tch.e[0].Tags[0]) //  "tag1" added
	assert.Equal(t, "tag2", tch.e[0].Tags[1]) //  "tag2" added
	assert.Equal(t, "", tch.e[0].Hostname)    // No hostname added
}

func TestTagEventHandlerAddsHostname(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{}, nil)
	e := &gostatsd.Event{
		SourceIP: "1.2.3.4",
	}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e))                // Metric tracked
	assert.Equal(t, 0, len(tch.e[0].Tags))        // No tags added
	assert.Equal(t, "1.2.3.4", tch.e[0].Hostname) // Hostname injected
}

func TestTagEventHandlerAddsDuplicateTags(t *testing.T) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{"tag1", "tag2", "tag2", "tag3", "tag1"}, nil)
	e := &gostatsd.Event{}
	th.DispatchEvent(context.Background(), e)
	assert.Equal(t, 1, len(tch.e))            // Metric tracked
	assert.Equal(t, 3, len(tch.e[0].Tags))    // 3 tags added
	assert.Equal(t, "tag1", tch.e[0].Tags[0]) //  "tag1" added
	assert.Equal(t, "tag2", tch.e[0].Tags[1]) //  "tag2" added
	assert.Equal(t, "tag3", tch.e[0].Tags[2]) //  "tag3" added
	assert.Equal(t, "", tch.e[0].Hostname)    // No hostname added
}

func BenchmarkTagMetricHandlerAddsDuplicateTagsSmall(b *testing.B) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}, nil)

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		m := &gostatsd.Metric{}
		th.DispatchMetric(context.Background(), m)
	}
}

func BenchmarkTagMetricHandlerAddsDuplicateTagsLarge(b *testing.B) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"cccccccccccccccccccccccccccccccc:cccccccccccccccccccccccccccccccc",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"cccccccccccccccccccccccccccccccc:cccccccccccccccccccccccccccccccc",
		"dddddddddddddddddddddddddddddddd:dddddddddddddddddddddddddddddddd",
		"dddddddddddddddddddddddddddddddd:dddddddddddddddddddddddddddddddd",
		"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
	}, nil)

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		m := &gostatsd.Metric{}
		th.DispatchMetric(context.Background(), m)
	}
}

func BenchmarkTagEventHandlerAddsDuplicateTagsSmall(b *testing.B) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}, nil)

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		e := &gostatsd.Event{}
		th.DispatchEvent(context.Background(), e)
	}
}

func BenchmarkTagEventHandlerAddsDuplicateTagsLarge(b *testing.B) {
	tch := &TagCapturingHandler{}
	th := NewTagHandler(tch, tch, gostatsd.Tags{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"cccccccccccccccccccccccccccccccc:cccccccccccccccccccccccccccccccc",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"cccccccccccccccccccccccccccccccc:cccccccccccccccccccccccccccccccc",
		"dddddddddddddddddddddddddddddddd:dddddddddddddddddddddddddddddddd",
		"dddddddddddddddddddddddddddddddd:dddddddddddddddddddddddddddddddd",
		"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
	}, nil)

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		e := &gostatsd.Event{}
		th.DispatchEvent(context.Background(), e)
	}
}
