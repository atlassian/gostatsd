package gostatsd

import (
	"sort"
	"strings"
)

// Tags represents a list of tags. Tags can be of two forms:
// 1. "key:value". "value" may contain column(s) as well.
// 2. "tag". No column.
// Each tag's key and/or value may contain characters invalid for a particular backend.
// Backends are expected to handle them appropriately. Different backends may have different sets of valid
// characters so it is undesirable to have restrictions on the input side.
type Tags []string

const (
	// StatsdSourceID stores the key used to tag metrics with the origin IP address.
	// Should be short to avoid extra hashing and memory overhead for map operations.
	StatsdSourceID = "s"
	unset          = "unknown"
)

// String returns a comma-separated string representation of the tags.
func (tags Tags) String() string {
	return strings.Join(tags, ",")
}

// SortedString sorts the tags alphabetically and returns
// a comma-separated string representation of the tags.
// Note that this method may mutate the original object.
func (tags Tags) SortedString() string {
	sort.Strings(tags)
	return tags.String()
}

// NormalizeTagKey cleans up the key of a tag.
func NormalizeTagKey(key string) string {
	return strings.Replace(key, ":", "_", -1)
}

// Concat returns a new Tags with the additional ones added
func (tags Tags) Concat(additional Tags) Tags {
	t := make(Tags, 0, len(tags)+len(additional))
	t = append(t, tags...)
	t = append(t, additional...)
	return t
}

// Copy returns a copy of the Tags
func (tags Tags) Copy() Tags {
	if tags == nil {
		return nil
	}
	tagCopy := make(Tags, len(tags))
	copy(tagCopy, tags)
	return tagCopy
}

// ToMap converts all the tags into a format that can be translated
// to send to a different vendor if required.
// - If the tag exists without a value it is converted to: "unknown:<tag>"
// - If the tag has several values it is converted to: "tag:Join(Sort(values), "__")"
//   - []{ "tag:pineapple","tag:pear" } ==> "tag:pear__pineapple"
//   - []{ "tag:newt" } ==> "tag:newt"
//   - []{ "tag:newt", "tag:newt" } ==> "tag:newt__newt"
//
// - If the tag key contains a . it is re-mapped to _
func (tags Tags) ToMap(host string) map[string]string {
	flatpack := make(map[string][]string, len(tags))
	for i := 0; i < len(tags); i++ {
		key, value := parseTag(tags[i])
		key = strings.ReplaceAll(key, `.`, `_`)
		flatpack[key] = append(flatpack[key], value)
	}

	tagsMap := make(map[string]string, len(flatpack))
	for key, values := range flatpack {
		// Cheap operation, due to the fact that no sorting or additional allocation is required.
		if len(values) == 1 {
			tagsMap[key] = values[0]
			continue
		}
		// Expensive operation due an values being sorted and additioanl string being created with the Join
		sort.Strings(values)
		tagsMap[key] = strings.Join(values, `__`)
	}

	if _, exist := tagsMap[`host`]; !exist && host != "" {
		tagsMap[`host`] = host
	}
	return tagsMap
}

func parseTag(tag string) (string, string) {
	tokens := strings.SplitN(tag, ":", 2)
	if len(tokens) == 2 {
		return tokens[0], tokens[1]
	}
	return unset, tokens[0]
}
