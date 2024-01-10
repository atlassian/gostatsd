package otlp

import (
	"context"

	"github.com/atlassian/gostatsd"
)

const (
	namedBackend = `otlp`
)

// Client contains additional meta data in order
// to export values as OTLP metrics.
// The zero value is not safe to use.
type Client struct {
	resources []string
}

var _ gostatsd.Backend = (*Client)(nil)

func (Client) Name() string { return namedBackend }

func (Client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	return nil
}

func (Client) SendMetricsAsync(ctx context.Context, mm *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	cb(nil)
}
