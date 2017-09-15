package statser

import (
	"context"
	"time"

	"github.com/atlassian/gostatsd"
)

// HeartBeater periodically sends a gauge for heartbeat purposes
type HeartBeater struct {
	client     Statser
	metricName string
	tags       gostatsd.Tags
	interval   time.Duration
}

// NewHeartBeater creates a new HeartBeater
func NewHeartBeater(client Statser, metricName string, tags gostatsd.Tags, interval time.Duration) *HeartBeater {
	return &HeartBeater{
		client:     client,
		metricName: metricName,
		tags:       tags,
		interval:   interval,
	}
}

// Run will run a HeartBeater in the background until the supplied context is
// closed, or Stop is called.
func (hb *HeartBeater) Run(ctx context.Context) {
	ticker := time.NewTicker(hb.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			beats++
			hb.emit()
		}
	}
}

func (hb *HeartBeater) emit() {
	hb.client.Gauge(hb.metricName, 0, hb.tags.Copy())
}
