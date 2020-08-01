package gostatsd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetricReset(t *testing.T) {
	// this is deliberately constructed without using named fields,
	// so that if the fields change it will cause a compiler error.
	m := &Metric{
		"metric",
		10,
		1,
		Tags{"tag"},
		"something",
		"somethingelse",
		"source",
		123,
		COUNTER,
		nil,
	}
	m.Reset()
	// Tags needs to be an empty slice, not a nil slice, because half the reason
	// behind having the MetricPool and Reset is to re-use the Tags slice. Therefore
	// we don't nil it.
	require.EqualValues(t, &Metric{Tags: Tags{}, Rate: 1}, m)
}

func TestMetricString(t *testing.T) {
	types := []MetricType{COUNTER, TIMER, SET, GAUGE, 42}
	names := []string{"counter", "timer", "set", "gauge", "unknown"}
	for idx, name := range names {
		require.Equal(t, name, types[idx].String())
	}
}

func TestUpdateTags(t *testing.T) {
	m := &Metric{}
	m.FormatTagsKey()
	require.EqualValues(t, "", m.TagsKey)
	m.AddTagsSetSource(Tags{"foo"}, "source")
	require.EqualValues(t, Tags{"foo"}, m.Tags)
	require.EqualValues(t, "source", m.Source)
	require.EqualValues(t, "", m.TagsKey)
	m.FormatTagsKey()
	require.EqualValues(t, "foo,s:source", m.TagsKey) // It's set
	m.AddTagsSetSource(Tags{"foo2"}, "source2")
	require.EqualValues(t, Tags{"foo", "foo2"}, m.Tags)
	require.EqualValues(t, "source2", m.Source)
	require.EqualValues(t, "", m.TagsKey) // It's cleared
	m.FormatTagsKey()
	require.EqualValues(t, "foo,foo2,s:source2", m.TagsKey)
}
