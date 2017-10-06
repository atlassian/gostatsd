package statsd

import (
	"context"
	"sync"
	"testing"

	"github.com/atlassian/gostatsd/pkg/fakesocket"
)

func BenchmarkReceive(b *testing.B) {
	mr := &DatagramReceiver{
		receiveBatchSize: 1,
	}
	c := fakesocket.NewFakePacketConn()
	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		mr.Receive(ctx, c)
	}
}
