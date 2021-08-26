package web_test

import (
	"context"

	"github.com/atlassian/gostatsd"
)

type channeledHandler struct {
	chMaps chan *gostatsd.MetricMap
}

func (ch *channeledHandler) EstimatedTags() int {
	return 0
}

func (ch *channeledHandler) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
	select {
	case <-ctx.Done():
	case ch.chMaps <- mm:
	}
}

func (ch *channeledHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	panic("events are not supported")
}

func (ch *channeledHandler) WaitForEvents() {
	panic("events are not supported")
}
