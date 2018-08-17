package gostatsd

import (
	"testing"

	"strings"

	"github.com/stretchr/testify/assert"
)

func TestNewStringMatch(t *testing.T) {
	tests := []struct {
		input    string
		expected StringMatch
	}{
		{"", StringMatch{test: "", invertMatch: false, prefixMatch: false}},
		{"*", StringMatch{test: "", invertMatch: false, prefixMatch: true}},
		{"!", StringMatch{test: "", invertMatch: true, prefixMatch: false}},
		{"!*", StringMatch{test: "", invertMatch: true, prefixMatch: true}},
		{"abc", StringMatch{test: "abc", invertMatch: false, prefixMatch: false}},
		{"abc*", StringMatch{test: "abc", invertMatch: false, prefixMatch: true}},
		{"!abc", StringMatch{test: "abc", invertMatch: true, prefixMatch: false}},
		{"!abc*", StringMatch{test: "abc", invertMatch: true, prefixMatch: true}},
	}

	for _, test := range tests {
		sm := NewStringMatch(test.input)
		assert.EqualValues(t, test.expected, sm)
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
