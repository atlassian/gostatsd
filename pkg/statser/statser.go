package statser

import (
	"time"

	"github.com/atlassian/gostatsd"
)

// Statser is the interface for sending metrics
type Statser interface {
	// NotifyFlush is called when a flush occurs.  It signals all known subscribers.
	NotifyFlush(d time.Duration)
	// RegisterFlush returns a channel which is notified when a flush occurs, and a function to perform cleanup of the
	// registration for clean shutdown.
	RegisterFlush() (<-chan time.Duration, func())

	Gauge(name string, value float64, tags gostatsd.Tags)
	Count(name string, amount float64, tags gostatsd.Tags)
	Increment(name string, tags gostatsd.Tags)
	TimingMS(name string, ms float64, tags gostatsd.Tags)
	TimingDuration(name string, d time.Duration, tags gostatsd.Tags)
	NewTimer(name string, tags gostatsd.Tags) *Timer
	WithTags(tags gostatsd.Tags) Statser
}
