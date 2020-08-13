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

func TestAddTagsSetSource(t *testing.T) {
	mCounter := Counter{}
	mCounter.AddTagsSetSource(Tags{"foo"}, "source")
	require.Equal(t, Tags{"foo"}, mCounter.Tags)

	mGauge := Gauge{}
	mGauge.AddTagsSetSource(Tags{"foo"}, "source")
	require.Equal(t, Tags{"foo"}, mGauge.Tags)

	mSet := Set{}
	mSet.AddTagsSetSource(Tags{"foo"}, "source")
	require.Equal(t, Tags{"foo"}, mSet.Tags)

	mTimer := Timer{}
	mTimer.AddTagsSetSource(Tags{"foo"}, "source")
	require.Equal(t, Tags{"foo"}, mTimer.Tags)
}
