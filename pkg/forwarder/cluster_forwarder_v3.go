package forwarder

import (
	"context"

	"github.com/atlassian/gostatsd"
)

type ClusterForwarder interface {
	DispatchMetricMapTo(ctx context.Context, mm *gostatsd.MetricMap, node string)
}
