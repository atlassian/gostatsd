package influxdb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/atlassian/gostatsd"
)

func TestFormatNameTags(t *testing.T) {
	tests := []struct {
		testName   string
		metricName string
		tags       gostatsd.Tags
		expected   string
	}{
		{"no-tag", "name", gostatsd.Tags{}, "name "},
		{"single-tag", "name", gostatsd.Tags{"a:b"}, "name,a=b "},
		{"multiple-tags", "name", gostatsd.Tags{"a:b", "c:d"}, "name,a=b,c=d "},
		{"unsorted-tags", "name", gostatsd.Tags{"c:d", "a:b"}, "name,a=b,c=d "},
		{"duplicate-tags", "name", gostatsd.Tags{"c:d", "c:d"}, "name,c=d__d "},
		{"sorted-duplicate-tags", "name", gostatsd.Tags{"c:d", "c:e"}, "name,c=d__e "},
		{"unsorted-duplicate-tags", "name", gostatsd.Tags{"c:e", "c:d"}, "name,c=d__e "},
		{"unnamed-tag", "name", gostatsd.Tags{"e"}, "name,unnamed=e "},
		{"unnamed-tags", "name", gostatsd.Tags{"e", "f"}, "name,unnamed=e__f "},
		{"duplicate-unnamed-tags", "name", gostatsd.Tags{"e", "e"}, "name,unnamed=e__e "},
		{"sorted-unnamed-tags", "name", gostatsd.Tags{"e", "f"}, "name,unnamed=e__f "},
		{"unsorted-unnamed-tags", "name", gostatsd.Tags{"f", "e"}, "name,unnamed=e__f "},
		{"explicit-unnamed-tags", "name", gostatsd.Tags{"unnamed:f", "e"}, "name,unnamed=e__f "},
		// actual escape testing is in another test, these just ensures it's applied to names, keys, and values
		{"escape-name", `na\me`, gostatsd.Tags{}, `na\\me `},
		{"escape-tag-key", "name", gostatsd.Tags{`\:b`}, `name,\\=b `},
		{"escape-tag-value", "name", gostatsd.Tags{`a:\`}, `name,a=\\ `},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			actual := formatNameTags(test.metricName, test.tags)
			assert.Equal(t, test.expected, actual)
		})
	}
}

func TestEscapeNameValues(t *testing.T) {
	tests := []struct {
		testName string
		input    string
		expected string
	}{
		{"new-line", "\n", `\n`},
		{"carriage-return", "\r", `\r`},
		{"space", ` `, `\ `},
		{"comma", `,`, `\,`},
		{"backslash", `\`, `\\`},
		{"tab", "\t", "\\t"},
		{"all", "a\nb\rc d,e\\f\tg", `a\nb\rc\ d\,e\\f\tg`},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			sb := &strings.Builder{}
			escapeNameToBuilder(sb, test.input)
			assert.Equal(t, test.expected, sb.String())
		})
	}
}

func TestEscapeTagValues(t *testing.T) {
	tests := []struct {
		testName string
		input    string
		expected string
	}{
		{"new-line", "\n", `\n`},
		{"carriage-return", "\r", `\r`},
		{"space", ` `, `\ `},
		{"comma", `,`, `\,`},
		{"backslash", `\`, `\\`},
		{"equals", `=`, `\=`},
		{"tab", "\t", "\\t"},
		{"all", "a\nb\rc d,e\\f=g\th", `a\nb\rc\ d\,e\\f\=g\th`},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			sb := &strings.Builder{}
			escapeTagToBuilder(sb, test.input)
			assert.Equal(t, test.expected, sb.String())
		})
	}
}

func TestEscapeFieldStrings(t *testing.T) {
	tests := []struct {
		testName string
		input    string
		expected string
	}{
		{"backslash", "\\", "\"\\\\\""},
		{"double-quote", `"`, "\"\\\"\""},
		{"all", "a\\b\"c", "\"a\\\\b\\\"c\""},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			sb := &strings.Builder{}
			escapeStringToBuilder(sb, test.input)
			assert.Equal(t, test.expected, sb.String())
		})
	}
}
