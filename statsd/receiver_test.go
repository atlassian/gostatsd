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
		"smp.rte:5|c|@0.1|#foo:bar,baz": {Name: "smp.rte", Value: 50, Type: types.COUNTER, Tags: types.Tags{Items: []string{"foo:bar", "baz"}}},
		"smp.rte:5|c|#foo:bar,baz":      {Name: "smp.rte", Value: 5, Type: types.COUNTER, Tags: types.Tags{Items: []string{"foo:bar", "baz"}}},
		"uniq.usr:joe|s":                {Name: "uniq.usr", StringValue: "joe", Type: types.SET},
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
}
