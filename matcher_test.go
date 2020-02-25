package gostatsd

import (
	"testing"

	"regexp"
	"strings"

	"github.com/stretchr/testify/assert"
)

// used to indicate presence or absence of a regex, the actual expression is not compared
var present = regexp.MustCompile(".")

func TestNewStringMatch(t *testing.T) {
	tests := []struct {
		input    string
		expected StringMatch
	}{
		{"", StringMatch{test: "", invertMatch: false, prefixMatch: false, regex: nil}},
		{"*", StringMatch{test: "", invertMatch: false, prefixMatch: true, regex: nil}},
		{"!", StringMatch{test: "", invertMatch: true, prefixMatch: false, regex: nil}},
		{"!*", StringMatch{test: "", invertMatch: true, prefixMatch: true, regex: nil}},
		{"abc", StringMatch{test: "abc", invertMatch: false, prefixMatch: false, regex: nil}},
		{"abc*", StringMatch{test: "abc", invertMatch: false, prefixMatch: true, regex: nil}},
		{"!abc", StringMatch{test: "abc", invertMatch: true, prefixMatch: false, regex: nil}},
		{"!abc*", StringMatch{test: "abc", invertMatch: true, prefixMatch: true, regex: nil}},
		{"regex:", StringMatch{test: "", invertMatch: false, prefixMatch: false, regex: present}},
		{"regex:.*", StringMatch{test: ".*", invertMatch: false, prefixMatch: false, regex: present}},
		{"!regex:", StringMatch{test: "", invertMatch: true, prefixMatch: false, regex: present}},
		{"!regex:.*", StringMatch{test: ".*", invertMatch: true, prefixMatch: false, regex: present}},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			sm := NewStringMatch(test.input)
			assert.EqualValues(t, test.expected.test, sm.test)
			assert.EqualValues(t, test.expected.invertMatch, sm.invertMatch)
			assert.EqualValues(t, test.expected.prefixMatch, sm.prefixMatch)
			assert.EqualValues(t, test.expected.regex != nil, sm.regex != nil)
		})
	}
}

func TestStringMatchExact(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"abc", true},
		{"ABC", false},
		{"abcd", false},
		{"zabc", false},
		{"", false},
	}

	sm := NewStringMatch("abc")
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			assert.EqualValues(t, test.expected, sm.Match(test.input))
		})
	}
}

func TestStringMatchRegex(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"abc.def.098", true},
		{"ABC.def.098", false},
		{"abc.123.098", false},
		{"123.def.098", true},
		{"zabc", false},
		{"", false},
	}

	sm := NewStringMatch("regex:[abc|123]\\.def\\.[\\d]")
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			assert.EqualValues(t, test.expected, sm.Match(test.input))
		})
	}
}

func TestStringMatchRegexInvert(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"abc.def.098", false},
		{"ABC.def.098", true},
		{"abc.123.098", true},
		{"123.def.098", false},
		{"zabc", true},
		{"", true},
	}

	sm := NewStringMatch("!regex:[abc|123]\\.def\\.[\\d]")
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			assert.EqualValues(t, test.expected, sm.Match(test.input))
		})
	}
}

func TestStringMatchBadRegexPanics(t *testing.T) {
	assert.Panics(t, func() { NewStringMatch("regex:([abc|123]\\.def\\.[\\d]") })
}

func TestStringMatchExactEmpty(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"abc", false},
		{"ABC", false},
		{"abcd", false},
		{"zabc", false},
		{"", true},
	}

	sm := NewStringMatch("")
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			assert.EqualValues(t, test.expected, sm.Match(test.input))
		})
	}
}

func TestStringMatchSuffix(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"abc", true},
		{"ABC", false},
		{"abcd", true},
		{"zabc", false},
		{"", false},
	}

	sm := NewStringMatch("abc*")
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			assert.EqualValues(t, test.expected, sm.Match(test.input))
		})
	}
}

func TestStringMatchSuffixEmpty(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"abc", true},
		{"ABC", true},
		{"abcd", true},
		{"zabc", true},
		{"", true},
	}

	sm := NewStringMatch("*")
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			assert.EqualValues(t, test.expected, sm.Match(test.input))
		})
	}
}

func TestStringMatchInvert(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"abc", false},
		{"ABC", true},
		{"abcd", true},
		{"zabc", true},
		{"", true},
	}

	sm := NewStringMatch("!abc")
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			assert.EqualValues(t, test.expected, sm.Match(test.input))
		})
	}
}

func TestStringMatchInvertEmpty(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"abc", true},
		{"ABC", true},
		{"abcd", true},
		{"zabc", true},
		{"", false},
	}

	sm := NewStringMatch("!")
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			assert.EqualValues(t, test.expected, sm.Match(test.input))
		})
	}
}

func TestStringMatchSuffixInvert(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"abc", false},
		{"ABC", true},
		{"abcd", false},
		{"zabc", true},
		{"", true},
	}

	sm := NewStringMatch("!abc*")
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			assert.EqualValues(t, test.expected, sm.Match(test.input))
		})
	}
}

func TestStringMatchSuffixInvertEmpty(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"abc", false},
		{"ABC", false},
		{"abcd", false},
		{"zabc", false},
		{"", false},
	}

	sm := NewStringMatch("!*")
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			assert.EqualValues(t, test.expected, sm.Match(test.input))
		})
	}
}

func TestStringMatchListAny(t *testing.T) {
	sml := StringMatchList{} // no filters matches nothing
	assert.Equal(t, false, sml.MatchAny("abc"))
	assert.Equal(t, false, sml.MatchAny("cba"))
	assert.Equal(t, false, sml.MatchAny("xyz"))
	assert.Equal(t, false, sml.MatchAny(""))

	sml = StringMatchList{
		NewStringMatch("abc"),
	}
	assert.Equal(t, true, sml.MatchAny("abc"))
	assert.Equal(t, false, sml.MatchAny("cba"))
	assert.Equal(t, false, sml.MatchAny("xyz"))
	assert.Equal(t, false, sml.MatchAny(""))

	sml = StringMatchList{
		NewStringMatch("abc"),
		NewStringMatch("cba"),
	}
	assert.Equal(t, true, sml.MatchAny("abc"))
	assert.Equal(t, true, sml.MatchAny("cba"))
	assert.Equal(t, false, sml.MatchAny("xyz"))
	assert.Equal(t, false, sml.MatchAny(""))
}

func TestStringMatchListAnyMultipleEmpty(t *testing.T) {
	tests := []struct {
		input    []string
		expected bool
	}{
		{[]string{}, false},
		{[]string{""}, false},
		{[]string{"abc"}, false},
		{[]string{"abc", "xyz"}, false},
		{[]string{"def", "abc"}, false},
		{[]string{"def", "ghi"}, false},
		{[]string{"def", "xyz"}, false},
	}

	sml := StringMatchList{} // no filters matches nothing
	for _, test := range tests {
		t.Run(strings.Join(test.input, ","), func(t *testing.T) {
			assert.EqualValues(t, test.expected, sml.MatchAnyMultiple(test.input))
		})
	}
}

func TestStringMatchListAnyMultipleSingle(t *testing.T) {
	tests := []struct {
		input    []string
		expected bool
	}{
		{[]string{}, false},
		{[]string{""}, false},
		{[]string{"abc"}, true},
		{[]string{"abc", "xyz"}, true},
		{[]string{"def", "abc"}, true},
		{[]string{"def", "ghi"}, false},
		{[]string{"def", "xyz"}, false},
	}

	sml := StringMatchList{
		NewStringMatch("abc"),
	}
	for _, test := range tests {
		t.Run(strings.Join(test.input, ","), func(t *testing.T) {
			assert.EqualValues(t, test.expected, sml.MatchAnyMultiple(test.input))
		})
	}
}

func TestStringMatchListAnyMultipleMultiple(t *testing.T) {
	tests := []struct {
		input    []string
		expected bool
	}{
		{[]string{}, false},
		{[]string{""}, false},
		{[]string{"abc"}, true},
		{[]string{"abc", "xyz"}, true},
		{[]string{"def", "abc"}, true},
		{[]string{"def", "ghi"}, true},
		{[]string{"def", "xyz"}, false},
	}

	sml := StringMatchList{
		NewStringMatch("abc"),
		NewStringMatch("ghi"),
	}
	for _, test := range tests {
		t.Run(strings.Join(test.input, ","), func(t *testing.T) {
			assert.EqualValues(t, test.expected, sml.MatchAnyMultiple(test.input))
		})
	}
}
