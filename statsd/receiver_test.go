package statsd

import (
	"reflect"
	"testing"

	"github.com/jtblin/gostatsd/types"
)

func TestParseLine(t *testing.T) {
	tests := map[string]types.Metric{
		"foo.bar.baz:2|c":               {Name: "foo.bar.baz", Value: 2, Type: types.COUNTER},
		"abc.def.g:3|g":                 {Name: "abc.def.g", Value: 3, Type: types.GAUGE},
		"def.g:10|ms":                   {Name: "def.g", Value: 10, Type: types.TIMER},
		"smp.rte:5|c|@0.1":              {Name: "smp.rte", Value: 50, Type: types.COUNTER},
		"smp.rte:5|c|@0.1|#foo:bar,baz": {Name: "smp.rte", Value: 50, Type: types.COUNTER, Tags: types.Tags{"foo:bar", "baz"}},
		"smp.rte:5|c|#foo:bar,baz":      {Name: "smp.rte", Value: 5, Type: types.COUNTER, Tags: types.Tags{"foo:bar", "baz"}},
		"uniq.usr:joe|s":                {Name: "uniq.usr", StringValue: "joe", Type: types.SET},
		"fooBarBaz:2|c":                 {Name: "fooBarBaz", Value: 2, Type: types.COUNTER},
		"smp.rte:5|c|#Foo:Bar,baz":      {Name: "smp.rte", Value: 5, Type: types.COUNTER, Tags: types.Tags{"foo:bar", "baz"}},
	}

	mr := &MetricReceiver{}

	for input, expected := range tests {
		result, err := mr.parseLine([]byte(input))
		if err != nil {
			t.Errorf("test %s error: %s", input, err)
			continue
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("test %s: expected %s, got %s", input, expected, result)
			continue
		}
	}

	failing := []string{"fOO|bar:bazkk", "foo.bar.baz:1|q"}
	for _, tc := range failing {
		result, err := mr.parseLine([]byte(tc))
		if err == nil {
			t.Errorf("test %s: expected error but got %s", tc, result)
		}
	}

	tests = map[string]types.Metric{
		"foo.bar.baz:2|c": {Name: "stats.foo.bar.baz", Value: 2, Type: types.COUNTER},
		"abc.def.g:3|g":   {Name: "stats.abc.def.g", Value: 3, Type: types.GAUGE},
		"def.g:10|ms":     {Name: "stats.def.g", Value: 10, Type: types.TIMER},
		"uniq.usr:joe|s":  {Name: "stats.uniq.usr", StringValue: "joe", Type: types.SET},
	}

	mr = &MetricReceiver{Namespace: "stats"}

	for input, expected := range tests {
		result, err := mr.parseLine([]byte(input))
		if err != nil {
			t.Errorf("test %s error: %s", input, err)
			continue
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("test %s: expected %s, got %s", input, expected, result)
			continue
		}
	}

	tests = map[string]types.Metric{
		"foo.bar.baz:2|c":         {Name: "foo.bar.baz", Value: 2, Type: types.COUNTER, Tags: types.Tags{"env:foo"}},
		"abc.def.g:3|g":           {Name: "abc.def.g", Value: 3, Type: types.GAUGE, Tags: types.Tags{"env:foo"}},
		"def.g:10|ms":             {Name: "def.g", Value: 10, Type: types.TIMER, Tags: types.Tags{"env:foo"}},
		"uniq.usr:joe|s":          {Name: "uniq.usr", StringValue: "joe", Type: types.SET, Tags: types.Tags{"env:foo"}},
		"uniq.usr:joe|s|#foo:bar": {Name: "uniq.usr", StringValue: "joe", Type: types.SET, Tags: types.Tags{"env:foo", "foo:bar"}},
	}

	mr = &MetricReceiver{Tags: []string{"env:foo"}}

	for input, expected := range tests {
		result, err := mr.parseLine([]byte(input))
		if err != nil {
			t.Errorf("test %s error: %s", input, err)
			continue
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("test %s: expected %s, got %s", input, expected, result)
			continue
		}
	}
}

func benchmarkParseLine(mr *MetricReceiver, input string, b *testing.B) {
	for n := 0; n < b.N; n++ {
		mr.parseLine([]byte(input))
	}
}

func BenchmarkParseLineCounter(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{}, "foo.bar.baz:2|c", b)
}
func BenchmarkParseLineCounterWithTagsAndSampleRate(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{}, "smp.rte:5|c|@0.1|#foo:bar,baz", b)
}
func BenchmarkParseLineGauge(b *testing.B) { benchmarkParseLine(&MetricReceiver{}, "abc.def.g:3|g", b) }
func BenchmarkParseLineTimer(b *testing.B) { benchmarkParseLine(&MetricReceiver{}, "def.g:10|ms", b) }
func BenchmarkParseLineSet(b *testing.B)   { benchmarkParseLine(&MetricReceiver{}, "uniq.usr:joe|s", b) }
func BenchmarkParseLineCounterWithDefaultTags(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{Tags: []string{"env:foo"}}, "foo.bar.baz:2|c", b)
}
func BenchmarkParseLineCounterWithDefaultTagsAndTags(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{Tags: []string{"env:foo"}}, "foo.bar.baz:2|c|#foo:bar,baz", b)
}
func BenchmarkParseLineCounterWithDefaultTagsAndTagsAndNameSpace(b *testing.B) {
	benchmarkParseLine(&MetricReceiver{Namespace: "stats", Tags: []string{"env:foo"}}, "foo.bar.baz:2|c|#foo:bar,baz", b)
}

func TestParseTags(t *testing.T) {
	mr := &MetricReceiver{}
	_, err := mr.parseTags("%foo:bar")
	if err == nil {
		t.Error("test %foo:bar: expected error but got nil")
	}

	result, err := mr.parseTags("#foo:bar,bar,baz:foo")
	if err != nil {
		t.Errorf("test #foo:bar,bar,baz:foo: unexpected error %s", err)
	}
	expected := types.Tags{"foo:bar", "bar", "baz:foo"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("test #foo:bar,bar,baz:foo: expected %s, got %s", expected, result)
	}
}

func BenchmarkParseTags(b *testing.B) {
	mr := &MetricReceiver{}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		mr.parseTags("#foo:bar,bar,baz:foo")
	}
}
