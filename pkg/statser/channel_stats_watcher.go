package statser

import (
	"context"
	"time"

	"github.com/atlassian/gostatsd"
)

// ChannelStatsWatcher reports metrics about channel usage to a Statser
type ChannelStatsWatcher struct {
	client   Statser
	tags     gostatsd.Tags
	capacity int
	lenFunc  func() int
	interval time.Duration
}

// NewChannelStatsWatcher creates a new ChannelStatsWatcher
func NewChannelStatsWatcher(client Statser, channelName string, tags gostatsd.Tags, capacity int, lenFunc func() int, interval time.Duration) *ChannelStatsWatcher {
	t := gostatsd.Tags{"channel:" + channelName}
	if tags != nil {
		t = append(t, tags...)
	}
	return &ChannelStatsWatcher{
		client:   client,
		tags:     t,
		capacity: capacity,
		lenFunc:  lenFunc,
		interval: interval,
	}
}

// Run will run a ChannelStatsWatcher in the background until the supplied context is
// closed, or Stop is called.
func (csw *ChannelStatsWatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(csw.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			csw.emit()
		}
	}
}

func (csw *ChannelStatsWatcher) emit() {
	capacity := float64(csw.capacity)
	queued := float64(csw.lenFunc())
	percentUsed := 100.0 * (queued / capacity)

	// tags are normally read from external sources, so are distinct even if the
	// same values.  As such, many things make assumptions about the ability to
	// modify tags in place.  So we duplicate them.
	csw.client.Gauge("channel.capacity", capacity, copyTags(csw.tags))
	csw.client.Gauge("channel.queued", queued, copyTags(csw.tags))
	csw.client.Gauge("channel.pct_used", percentUsed, copyTags(csw.tags))
}
