package statsd

import (
	"testing"

	"github.com/atlassian/gostatsd/tester/fakesocket"
	"github.com/atlassian/gostatsd/types"

	"golang.org/x/net/context"
)

var receiveBlackhole error

func BenchmarkReceive(b *testing.B) {
	mr := &metricReceiver{
		handler: nopHandler{},
	}
	c := fakesocket.FakePacketConn{}
	var r error
	for n := 0; n < b.N; n++ {
		r = mr.Receive(context.Background(), c)
	}
	receiveBlackhole = r
}

type nopHandler struct{}

func (h nopHandler) DispatchMetric(ctx context.Context, m *types.Metric) error {
	return context.Canceled // Stops receiver after first read is done
}

func (h nopHandler) DispatchEvent(ctx context.Context, e *types.Event) error {
	return context.Canceled // Stops receiver after first read is done
}
