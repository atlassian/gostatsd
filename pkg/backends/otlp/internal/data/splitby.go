package data

import (
	"strings"
)

func splitTagsByKeys[S ~[]string](tags S, keys S) (matched, unmatched S) {
	split := 0
	if len(keys) != 0 && len(tags) != 0 {
		for k := 0; k < len(keys); k++ {
			for t := split; t < len(tags); t++ {
				if strings.HasPrefix(tags[t], keys[k]+":") {
					tags[split], tags[t] = tags[t], tags[split]
					split++
				}
			}
		}
	}
	return tags[:split], tags[split:]
}

type Converter[S ~[]string] func(S) Map

func SplitTagsByKeysAndConvert[S ~[]string](tags S, keys S, converter Converter[S]) (matched, unmatched Map) {
	matchedTags, unmatchedTags := splitTagsByKeys(tags, keys)
	return converter(matchedTags), converter(unmatchedTags)
}

func SplitMetricTagsByKeys[S ~[]string](tags S, keys S) (matched, unmatched Map) {
	converter := func(s S) Map {
		return NewMap(WithStatsdDelimitedTags(s))
	}
	return SplitTagsByKeysAndConvert(tags, keys, converter)
}

func SplitEventTagsByKeys[S ~[]string](tags S, keys S) (matched, unmatched Map) {
	converter := func(s S) Map {
		return NewMap(WithStatsdEventTags(s))
	}
	return SplitTagsByKeysAndConvert(tags, keys, converter)
}
