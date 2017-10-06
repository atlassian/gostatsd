package statsd

import (
	"context"
	"testing"
	"time"

	"github.com/atlassian/gostatsd/pkg/fakesocket"
	"github.com/magiconair/properties/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkReceive(b *testing.B) {
	ch := make(chan []*Datagram, 1)
	mr := NewDatagramReceiver(ch, 1)
	c := fakesocket.NewFakePacketConn()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Stops receiver after first read is done

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		mr.Receive(ctx, c)
	}
}

func TestDatagramReceiver_Receive(t *testing.T) {
	ch := make(chan []*Datagram, 1)
	mr := NewDatagramReceiver(ch, 2)
	c := fakesocket.NewFakePacketConn()

	ctx, cancel := context.WithCancel(context.Background())

	go mr.Receive(ctx, c)

	var dgs []*Datagram
	select {
	case dgs = <-ch:
	case <-time.After(time.Second):
		t.Errorf("Timeout, failed to read datagram")
	}

	cancel()

	require.Len(t, dgs, 1)

	dg := dgs[0]
	assert.Equal(t, string(dg.IP), fakesocket.FakeAddr.IP.String())
	assert.Equal(t, dg.Msg, fakesocket.FakeMetric)
}
