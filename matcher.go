package gostatsd

import (
	"regexp"
	"strings"
)

type StringMatch struct {
	test        string
	invertMatch bool
	prefixMatch bool
	regex       *regexp.Regexp
}

type StringMatchList []StringMatch

func NewStringMatch(s string) StringMatch {
	prefix := false
	invert := strings.HasPrefix(s, "!")
	if invert {
		s = s[1:]
	}

	var compiledRegex *regexp.Regexp = nil
	if strings.HasPrefix(s, "regex:") {
		s = s[6:]
		compiledRegex = regexp.MustCompile(s)
	} else if strings.HasSuffix(s, "*") {
		prefix = true
		s = s[0 : len(s)-1]
	}
	return StringMatch{
		test:        s,
		invertMatch: invert,
		prefixMatch: prefix,
		regex:       compiledRegex,
	}
}

// Match indicates if the provided string matches the criteria for this StringMatch
func (sm StringMatch) Match(s string) bool {
	switch {
	case sm.regex != nil:
		return sm.regex.MatchString(s) != sm.invertMatch
	case sm.prefixMatch:
		return strings.HasPrefix(s, sm.test) != sm.invertMatch
	default: // exact match
		return (s == sm.test) != sm.invertMatch
	}
}

// MatchAny indicates if s matches anything in the list, returns false if the list is empty
func (sml StringMatchList) MatchAny(s string) bool {
	for _, sm := range sml {
		if sm.Match(s) {
			return true
		}
	}
	return false
}

// MatchMultipleAny indicates if any string passed matches anything in the list, returns false if
// sml or tests is empty
func (sml StringMatchList) MatchAnyMultiple(tests []string) bool {
	for _, s := range tests {
		if sml.MatchAny(s) {
			return true
		}
	}
	return false
}
