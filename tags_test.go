package gostatsd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTagsToMap(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		host     string
		tags     []string
		expected map[string]string
	}{
		{"empty tags", "", nil, map[string]string{}},
		{"host is added", "x", nil, map[string]string{"host": "x"}},
		{"1 tag is added", "", []string{"a:b"}, map[string]string{"a": "b"}},
		{"2 tags are added", "", []string{"a:b", "c:d"}, map[string]string{"a": "b", "c": "d"}},
		{"missing keys are handled", "", []string{"b", "c:d"}, map[string]string{"unknown": "b", "c": "d"}},
		{"conflicting keys are handled", "", []string{"a:b", "a:d"}, map[string]string{"a": "b__d"}},
		{"host in tag is truth", "x", []string{"host:b"}, map[string]string{"host": "b"}},
		{"multiple unknown sources", "", []string{"b", "unknown:c"}, map[string]string{"unknown": "b__c"}},
		{"conflicts are sorted 1", "", []string{"a:b", "a:c"}, map[string]string{"a": "b__c"}},
		{"conflicts are sorted 2", "", []string{"a:c", "a:b"}, map[string]string{"a": "b__c"}},
		{"multiple colons are ok", "", []string{"a:b:c"}, map[string]string{"a": "b:c"}},
		{"dots are removed from keys", "", []string{"a.b:c"}, map[string]string{"a_b": "c"}},
		{"dots are not removed from values", "", []string{"a:b.c"}, map[string]string{"a": "b.c"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tags := Tags(tc.tags)
			assert.EqualValues(t, tc.expected, tags.ToMap())
		})
	}
}
