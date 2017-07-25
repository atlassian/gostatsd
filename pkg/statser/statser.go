package statser

import (
	"time"

	"github.com/atlassian/gostatsd"
)

// Statser is the interface for sending metrics
type Statser interface {
	Gauge(name string, value float64, tags gostatsd.Tags)
	Count(name string, amount float64, tags gostatsd.Tags)
	Increment(name string, tags gostatsd.Tags)
	TimingMS(name string, ms float64, tags gostatsd.Tags)
	TimingDuration(name string, d time.Duration, tags gostatsd.Tags)
	NewTimer(name string, tags gostatsd.Tags) *Timer
	WithTags(tags gostatsd.Tags) Statser
}

func concatTags(a, b gostatsd.Tags) gostatsd.Tags {
	t := make(gostatsd.Tags, 0, len(a)+len(b))
	t = append(t, a...)
	t = append(t, b...)
	return t
}

func copyTags(tags gostatsd.Tags) gostatsd.Tags {
	new := make(gostatsd.Tags, len(tags))
	copy(new, tags)
	return new
}
