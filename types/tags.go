package types

import (
	"sort"
	"strings"
)

// Tags represents a list of tags.
type Tags []string

// StatsdSourceID stores the key used to tag metrics with the origin IP address.
// Should be short to avoid extra hashing and memory overhead for map operations.
const StatsdSourceID = "s"

var tagKeysReplacer = strings.NewReplacer(":", "_", ",", "_")

// String returns a comma-separated string representation of the tags.
func (tags Tags) String() string {
	return strings.Join(tags, ",")
}

// SortedString sorts the tags alphabetically and returns
// a comma-separated string representation of the tags.
func (tags Tags) SortedString() string {
	sort.Strings(tags)
	return strings.Join(tags, ",")
}

// NormalizeTagKey cleans up the key of a tag.
func NormalizeTagKey(key string) string {
	return tagKeysReplacer.Replace(key)
}

// NormalizeTagValue cleans up the value of a tag.
func NormalizeTagValue(value string) string {
	return strings.Replace(value, ",", "_", -1)
}
