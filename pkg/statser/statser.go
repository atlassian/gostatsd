package statser

import (
	"time"

	"github.com/atlassian/gostatsd"
)

// Statser is the interface for sending metrics
type Statser interface {
	NotifyFlush(d time.Duration)
	RegisterFlush() (<-chan time.Duration, func())

	Gauge(name string, value float64, tags gostatsd.Tags)
	Count(name string, amount float64, tags gostatsd.Tags)
	Increment(name string, tags gostatsd.Tags)
	TimingMS(name string, ms float64, tags gostatsd.Tags)
	TimingDuration(name string, d time.Duration, tags gostatsd.Tags)
	NewTimer(name string, tags gostatsd.Tags) *Timer
	WithTags(tags gostatsd.Tags) Statser
}
