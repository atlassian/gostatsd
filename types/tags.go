package types

import (
	"sort"
	"strings"
)

// Tags represents a list of tags.
type Tags []string

var tagKeysReplacer = strings.NewReplacer(":", "_", ",", "_")

// String sorts the tags alphabetically and returns
// a comma-separated string representation of the tags.
func (tags Tags) String() string {
	sort.Strings(tags)
	return strings.Join(tags, ",")
}

// IndexOfKey returns the index and the element starting with the string key.
func (tags Tags) IndexOfKey(key string) (int, string) {
	key = key + ":"
	for i, v := range tags {
		if strings.HasPrefix(v, key) {
			return i, v
		}
	}
	return -1, ""
}

// NormalizeTagKey cleans up the key of a tag.
func NormalizeTagKey(key string) string {
	return tagKeysReplacer.Replace(key)
}

// NormalizeTagValue cleans up the value of a tag.
func NormalizeTagValue(value string) string {
	return strings.Replace(value, ",", "_", -1)
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
