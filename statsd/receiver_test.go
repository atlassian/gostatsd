package statsd

import (
	"reflect"
	"testing"

	"github.com/jtblin/gostatsd/types"
)

func TestParseLine(t *testing.T) {
	tests := map[string]types.Metric{
		"foo.bar.baz:2|c": {Bucket: "foo.bar.baz", Value: 2.0, Type: types.COUNTER, Tags: []types.Tag{}},
		"abc.def.g:3|g":   {Bucket: "abc.def.g", Value: 3, Type: types.GAUGE, Tags: []types.Tag{}},
		"def.g:10|ms":     {Bucket: "def.g", Value: 10, Type: types.TIMER, Tags: []types.Tag{}},
	}

	for input, expected := range tests {
		result, err := parseLine([]byte(input))
		if err != nil {
			t.Errorf("test %s error: %s", input, err)
			continue
		}
		if reflect.DeepEqual(result, expected) {
			t.Errorf("test %s: expected %s, got %s", input, expected, result)
			continue
		}
	}

	failing := []string{"fOO|bar:bazkk", "foo.bar.baz:1|q"}
	for _, tc := range failing {
		result, err := parseLine([]byte(tc))
		if err == nil {
			t.Errorf("test %s: expected error but got %s", tc, result)
		}
	}
}
