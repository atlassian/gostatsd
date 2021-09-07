package promremotewriter

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pb"
)

func TestCoerceToNumeric(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		arg  float64
		want float64
	}{
		{"NaN should return 0", math.NaN(), -1},
		{"+Inf should return maximum float value", math.Inf(+1), math.MaxFloat64},
		{"-Inf should return minimum float value", math.Inf(-1), -math.MaxFloat64},
		{"Zero value should return unchanged", 0, 0},
		{"Positive value within float64 range should return unchanged", 12_345, 12_345},
		{"Negative value within float64 should return unchanged", -12_345, -12_345},
		{"Maximum float64 value should return unchanged", math.MaxFloat64, math.MaxFloat64},
		{"Minimum float64 value should return unchanged", -math.MaxFloat64, -math.MaxFloat64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := coerceToNumeric(tt.arg); got != tt.want {
				t.Errorf("coerceToNumeric() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTagTranslation(t *testing.T) {
	tests := []struct {
		name   string
		in     gostatsd.Tags
		labels []*pb.PromLabel
	}{
		// 1a. tags that aren't key:value ("value") are normalized as "unnamed:value"
		// 1b. tags with no key (":value") are normalized as "unnamed:value"
		// 1c. tags with no value ("key:") are normalized as "key:__unset__"
		{"no separator", gostatsd.Tags{"foo"}, []*pb.PromLabel{{Name: "unnamed", Value: "foo"}}},
		{"missing value", gostatsd.Tags{"foo:"}, []*pb.PromLabel{{Name: "foo", Value: "__unset__"}}},
		{"missing key", gostatsd.Tags{":foo"}, []*pb.PromLabel{{Name: "unnamed", Value: "foo"}}},

		// 2. if there is a tag of the form "unnamed:x", it will be combined with the regular unnamed values
		{"combine unknown", gostatsd.Tags{"unnamed:abc", "def", ":ghi"}, []*pb.PromLabel{{Name: "unnamed", Value: "abc__def__ghi"}}},

		// 3. duplicate values for the same key are sorted (including "unnamed" tags)
		{"", gostatsd.Tags{"unnamed:abc", "ghi", ":def"}, []*pb.PromLabel{{Name: "unnamed", Value: "abc__def__ghi"}}},

		// 4. keys with multiple values have their values concatenated with __
		{"", gostatsd.Tags{"foo:abc", "foo:def"}, []*pb.PromLabel{{Name: "foo", Value: "abc__def"}}},

		// 5. the final output is sorted by keys
		{"", gostatsd.Tags{"def:def", "abc:abc", "mno:mno"}, []*pb.PromLabel{
			{Name: "abc", Value: "abc"},
			{Name: "def", Value: "def"},
			{Name: "mno", Value: "mno"},
		}},

		// Test everything, with some invalid data for good measure
		{"", gostatsd.Tags{"foo", "foo:unset", "baz:foo", "foo:", "unnamed:foo", "bar", "foo_:rab", "baz:bar", "foo?:bar"}, []*pb.PromLabel{
			{Name: "baz", Value: "bar__foo"},
			{Name: "foo", Value: "__unset____unset"},
			{Name: "foo_", Value: "bar__rab"},
			{Name: "unnamed", Value: "bar__foo__foo"},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			labels := tagsAsLabels("", tt.in)
			labels = labels[1:] // Drop the name
			require.Equal(t, tt.labels, labels)

		})
	}
}

// Tests for requirements from the standard

func TestMetricHasNameLabel(t *testing.T) {
	// SHOULD contain a __name__ label.
	t.Parallel()
	actual := tagsAsLabels("foo", gostatsd.Tags{})
	expected := []*pb.PromLabel{
		{Name: "__name__", Value: "foo"},
	}
	require.Equal(t, expected, actual)
}

func TestNoRepeatLabelNames(t *testing.T) {
	// MUST NOT contain repeated label names.
	t.Parallel()
	actual := tagsAsLabels("foo", gostatsd.Tags{"label:value", "label:value2"})
	expected := []*pb.PromLabel{
		{Name: "__name__", Value: "foo"},
		{Name: "label", Value: "value__value2"},
	}
	require.Equal(t, expected, actual)
}

func TestSortedLabelNames(t *testing.T) {
	// MUST have label names sorted in lexicographical order.
	t.Parallel()
	actual := tagsAsLabels("foo", gostatsd.Tags{"zyx:zyx", "abc:abc", "mno:mno"})
	expected := []*pb.PromLabel{
		{Name: "__name__", Value: "foo"},
		{Name: "abc", Value: "abc"},
		{Name: "mno", Value: "mno"},
		{Name: "zyx", Value: "zyx"},
	}
	require.Equal(t, expected, actual)
}

func TestNoEmptyLabelNamesOrValues(t *testing.T) {
	// MUST NOT contain any empty label names or values.
	t.Parallel()
	actual := tagsAsLabels("foo", gostatsd.Tags{"nosep", "novalue:", ":nokey"})
	expected := []*pb.PromLabel{
		{Name: "__name__", Value: "foo"},
		{Name: "novalue", Value: "__unset__"},
		{Name: "unnamed", Value: "nokey__nosep"},
	}
	require.Equal(t, expected, actual)
}

func TestMetricNamesValid(t *testing.T) {
	// Metric names MUST adhere to the regex `[a-zA-Z_:]([a-zA-Z0-9_:])*`
	t.Parallel()
	require.Regexp(t, "^[a-zA-Z_:]([a-zA-Z0-9_:])*$", sanitizeMetricName("stats.foo_123456789!@#$%^&*()_+>?"))
}

func TestLabelNamesValid(t *testing.T) {
	// Label names MUST adhere to the regex `[a-zA-Z_]([a-zA-Z0-9_])*`
	t.Parallel()
	require.Regexp(t, "^[a-zA-Z_]([a-zA-Z0-9_])*$", sanitizeMetricName("stats.foo_12345678!@#$%^&*()_+>?"))
}

func BenchmarkSanitizeMetricName(b *testing.B) {
	for _, metric := range []string{"statsd_my_metric_no_changes", "statsd.my.metric.some.changes", "s.t.a.t.s.d.m.y.m.e.t.r.i.c.m.a.n.y.c.h.a.n.g.e.s"} {
		b.Run(metric, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sanitizeMetricName(metric)
			}
		})
	}
}

func BenchmarkSanitizeLabelName(b *testing.B) {
	for _, metric := range []string{"statsd_my_metric_no_changes", "statsd.my.metric.some.changes", "s.t.a.t.s.d.m.y.m.e.t.r.i.c.m.a.n.y.c.h.a.n.g.e.s"} {
		b.Run(metric, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sanitizeLabelName(metric)
			}
		})
	}
}
