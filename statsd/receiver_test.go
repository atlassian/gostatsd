package statsd

import (
	"testing"
)

func TestParseLine(t *testing.T) {
	tests := map[string]Metric{
		"foo.bar.baz:2|c": Metric{Bucket: "foo.bar.baz", Value: 2.0, Type: COUNTER},
		"abc.def.g:3|g":   Metric{Bucket: "abc.def.g", Value: 3, Type: GAUGE},
		"def.g:10|ms":     Metric{Bucket: "def.g", Value: 10, Type: TIMER},
	}

	for input, expected := range tests {
		result, err := parseLine([]byte(input))
		if err != nil {
			t.Errorf("test %s error: %s", input, err)
			continue
		}
		if result != expected {
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
