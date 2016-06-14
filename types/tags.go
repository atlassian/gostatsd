package types

import (
	"sort"
	"strings"
)

// Tags represents a list of tags.
type Tags []string

// String sorts the tags alphabetically and returns
// a comma-separated string representation of the tags.
func (tags Tags) String() string {
	sort.Strings(tags)
	return strings.Join(tags, ",")
}

// Map returns a map of the tags.
func (tags Tags) Map() map[string]string {
	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		s := strings.Split(tag, ":")
		tagMap[s[0]] = ""
		if len(s) > 1 {
			tagMap[s[0]] = s[1]
		}
	}
	return tagMap
}

// IndexOfKey returns the index and the element starting with the string key.
func (tags Tags) IndexOfKey(key string) (int, string) {
	for i, v := range tags {
		if strings.HasPrefix(v, key+":") {
			return i, v
		}
	}
	return -1, ""
}

// Normalise normalises tags as key:value.
func (tags Tags) Normalise() Tags {
	nTags := make(Tags, 0, len(tags))
	for _, tag := range tags {
		if tag != "" {
			if !strings.Contains(tag, ":") {
				tag = "tag:" + tag
			}
			nTags = append(nTags, tag)
		}
	}
	return nTags
}

// TagToMetricName transforms tags into metric names.
func TagToMetricName(tag string) string {
	return regSemiColon.ReplaceAllString(tag, ".")
}

// NormalizeTagElement cleans up the key or the value of a tag.
func NormalizeTagElement(name string) string {
	element := regSemiColon.ReplaceAllString(name, "_")
	element = regDot.ReplaceAllString(element, "_")
	return strings.ToLower(element)
}

// ExtractSourceFromTags returns the source from the tags
// and the updated tags.
func ExtractSourceFromTags(s string) (string, Tags) {
	tags := Tags(strings.Split(s, ","))
	idx, element := tags.IndexOfKey(StatsdSourceID)
	if idx != -1 {
		bits := strings.Split(element, ":")
		if len(bits) > 1 {
			return bits[1], append(tags[:idx], tags[idx+1:]...)
		}
	}
	return "", tags
}
