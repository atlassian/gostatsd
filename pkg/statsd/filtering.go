package statsd

import (
	"github.com/atlassian/gostatsd"

	"github.com/spf13/viper"
)

type Filter struct {
	MatchMetrics   gostatsd.StringMatchList // Name must match
	ExcludeMetrics gostatsd.StringMatchList // Name must not match
	MatchTags      gostatsd.StringMatchList // Any tag must match
	DropTags       gostatsd.StringMatchList // Any tag matching anything will be dropped
	DropMetric     bool                     // Drop the entire metric
	DropHost       bool                     // Clears Hostname if present
}

// toStringMatch turns a []string in to a []gostatsd.StringMatch
func toStringMatch(tests []string) []gostatsd.StringMatch {
	var matches []gostatsd.StringMatch
	for _, test := range tests {
		matches = append(matches, gostatsd.NewStringMatch(test))
	}
	return matches
}

// NewFilterFromViper creates a new Filter given a *viper.Viper
func NewFilterFromViper(name string, v *viper.Viper) Filter {
	v.SetDefault("match-metrics", []string{})
	v.SetDefault("exclude-metrics", []string{})
	v.SetDefault("match-tags", []string{})
	v.SetDefault("drop-tags", []string{})
	v.SetDefault("drop-host", false)
	v.SetDefault("drop-metric", false)
	return Filter{
		MatchMetrics:   toStringMatch(v.GetStringSlice("match-metrics")),
		ExcludeMetrics: toStringMatch(v.GetStringSlice("exclude-metrics")),
		MatchTags:      toStringMatch(v.GetStringSlice("match-tags")),
		DropTags:       toStringMatch(v.GetStringSlice("drop-tags")),
		DropHost:       v.GetBool("drop-host"),
		DropMetric:     v.GetBool("drop-metric"),
	}
}
