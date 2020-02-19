package gostatsd

import (
	"regexp"
	"strings"
)

type StringMatch struct {
	test        string
	invertMatch bool
	prefixMatch bool
	isRegex     bool
	regex       *regexp.Regexp
}

type StringMatchList []StringMatch

func NewStringMatch(s string) StringMatch {

	invert := false
	if strings.HasPrefix(s, "!") {
		invert = true
		s = s[1:]
	}

	regex := false
	var compiledRegex *regexp.Regexp
	compiledRegex = nil
	if strings.HasPrefix(s, "regex:") {
		regex = true
		s = s[6:]
		compiledRegex, _ = regexp.Compile(s)
	}
	prefix := false
        if !regex {
		if strings.HasSuffix(s, "*") {
			prefix = true
			s = s[0 : len(s)-1]
		}
        }
	return StringMatch{
		test:        s,
		invertMatch: invert,
		prefixMatch: prefix,
		isRegex:     regex,
		regex:       compiledRegex,
	}
}

// Match indicates if the provided string matches the criteria for this StringMatch
func (sm StringMatch) Match(s string) bool {
	if sm.isRegex {
		return (sm.regex.MatchString(s)) != sm.invertMatch
	} else {
		if sm.prefixMatch {
			return strings.HasPrefix(s, sm.test) != sm.invertMatch
		}
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
