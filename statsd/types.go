package statsd

import (
	"github.com/atlassian/gostatsd/types"

	"golang.org/x/net/context"
)

// Handler interface can be used to handle metrics and events.
type Handler interface {
	DispatchMetric(context.Context, *types.Metric) error
	DispatchEvent(context.Context, *types.Event) error
}
