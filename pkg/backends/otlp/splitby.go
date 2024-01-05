package otlp

import (
	"strings"

	"github.com/atlassian/gostatsd/pkg/backends/otlp/internal/data"
)

func splitTagsByKeys[S ~[]string](tags S, keys S) (matched, unmatched data.Map) {
	if len(keys) == 0 || len(tags) == 0 {
		return data.NewMap(), data.NewMap(data.WithStatsdDelimitedTags(tags))
	}
	split := 0
	for k := 0; k < len(keys); k++ {
		for t := split; t < len(tags); t++ {
			if strings.HasPrefix(tags[t], keys[k]+":") {
				tags[split], tags[t] = tags[t], tags[split]
				split++
			}
		}
	}
	return data.NewMap(
			data.WithStatsdDelimitedTags(tags[:split]),
		),
		data.NewMap(
			data.WithStatsdDelimitedTags(tags[split:]),
		)

}
