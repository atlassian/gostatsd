package fixtures

import (
	"fmt"

	"github.com/atlassian/gostatsd"
)

type MetricOpt func(m *gostatsd.Metric)

// MakeMetric provides a way to build a metric for tests.  Hopefully over
// time this will be used more, bringing more consistency to tests.
func MakeMetric(opts ...MetricOpt) *gostatsd.Metric {
	m := &gostatsd.Metric{
		Type: gostatsd.COUNTER,
		Name: "name",
		Rate: 1,
		Tags: gostatsd.Tags{
			"foo:bar",
			"host:baz",
		},
		Source: "baz",
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func Name(n string) MetricOpt {
	return func(m *gostatsd.Metric) {
		m.Name = n
	}
}

func AddTag(t ...string) MetricOpt {
	return func(m *gostatsd.Metric) {
		m.Tags = append(m.Tags, t...)
	}
}

func DropSource(m *gostatsd.Metric) {
	m.Source = gostatsd.UnknownSource
}

func DropTag(t string) MetricOpt {
	return func(m *gostatsd.Metric) {
		next := 0
		found := false
		for _, tag := range m.Tags {
			if t == tag {
				found = true
				m.Tags[next] = tag
				next++
			}
		}
		if !found {
			panic(fmt.Sprintf("failed to find tag %s while building metric", t))
		}
		m.Tags = m.Tags[:next]
	}
}

// SortCompare func for metrics so they can be compared with require.EqualValues
// Invoke with sort.Slice(x, SortCompare(x))
func SortCompare(ms []*gostatsd.Metric) func(i, j int) bool {
	return func(i, j int) bool {
		if ms[i].Name == ms[j].Name {
			if len(ms[i].Tags) == len(ms[j].Tags) { // This is not exactly accurate, but close enough with our data
				if ms[i].Type == gostatsd.SET {
					return ms[i].StringValue < ms[j].StringValue
				} else {
					return ms[i].Value < ms[j].Value
				}
			}
			return len(ms[i].Tags) < len(ms[j].Tags)
		}
		return ms[i].Name < ms[j].Name
	}
}
