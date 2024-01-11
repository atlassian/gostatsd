package otlp

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/transport"
)

const (
	namedBackend = `otlp`
)

// Client contains additional meta data in order
// to export values as OTLP metrics.
// The zero value is not safe to use.
type Client struct{}

var _ gostatsd.Backend = (*Client)(nil)

func NewClientFromViper(v *viper.Viper, logger logrus.FieldLogger, pool *transport.TransportPool) (gostatsd.Backend, error) {
	_, err := NewConfig(v)
	if err != nil {
		return nil, err
	}

	return Client{}, nil
}

func (Client) Name() string { return namedBackend }

func (Client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	return nil
}

func (Client) SendMetricsAsync(ctx context.Context, mm *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	cb(nil)
}
