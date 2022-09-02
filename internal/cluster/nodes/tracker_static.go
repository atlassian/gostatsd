package nodes

import (
	"context"

	"github.com/atlassian/gostatsd"
)

// staticNodeTracker is a tracker with a static list of hosts
type staticNodeTracker struct{}

func NewStaticNodeTracker(picker consistentNodePicker, host []string) gostatsd.Runnable {
	for _, host := range host {
		picker.Add(host)
	}
	return (&staticNodeTracker{}).Run
}

func (sn *staticNodeTracker) Run(ctx context.Context) {}
