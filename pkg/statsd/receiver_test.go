package statsd

import (
	"context"
	"testing"

	"github.com/atlassian/gostatsd/pkg/fakesocket"
	"github.com/atlassian/gostatsd/pkg/statser"
)

func BenchmarkReceive(b *testing.B) {
	ch := make(chan *Datagram, 1)
	mr := NewDatagramReceiver(ch, 1, statser.NewNullStatser())
	c := fakesocket.NewFakePacketConn()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Stops receiver after first read is done

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		mr.Receive(ctx, c)
	}
}
