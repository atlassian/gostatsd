package statsd

import (
	"context"
	"net"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/magiconair/properties/assert"
	tassert "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/fakesocket"
)

func BenchmarkReceive(b *testing.B) {
	if testing.Short() {
		// This currently has trouble on arm64, needs to be investigated per #384
		b.Skip()
	}
	// Small values result in the channel aggressively blocking and causing slowdowns.
	// Large values result in the channel consuming lots of memory before the scheduler
	// gets to it, causing GC related slowdowns.
	//
	// ... so this is pretty arbitrary.
	ch := make(chan []*Datagram, 5000)
	mr := NewDatagramReceiver(ch, nil, 0, gostatsd.DefaultReceiveBatchSize)
	c, done := fakesocket.NewCountedFakePacketConn(uint64(b.N))

	var wg sync.WaitGroup
	// runtime.GOMAXPROCS() is listed as "will go away".
	// The **intent** is to get the current -cpu value per:
	// https://github.com/golang/go/blob/release-branch.go1.9/src/testing/benchmark.go#L430
	numProcs := runtime.GOMAXPROCS(0)
	wg.Add(numProcs + 1)
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case dgs := <-ch:
				for _, dg := range dgs {
					dg.DoneFunc()
				}
			case <-ctx.Done():
				wg.Done()
				return
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < numProcs; i++ {
		go func() {
			// mr.Receive() will loop until it runs out of packets to read, then it will
			// start having errors because the socket is closed.  When the socket is closed,
			// the read of the done channel will return allowing the context to be cancelled.
			mr.Receive(ctx, c)
			wg.Done()
		}()
	}

	<-done
}

func TestDatagramReceiver_Receive(t *testing.T) {
	ch := make(chan []*Datagram, 1)
	mr := NewDatagramReceiver(ch, nil, 0, 2)
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

func TestDatagramReceiver_UnixSocketConnection(t *testing.T) {
	ch := make(chan []*Datagram, 1)
	message := "abc.def.g:10|c"

	// Datagram receiver listening in Unix Domain Socket
	socketPath := os.TempDir() + "/gostatsd_receiver_test_receive_uds.sock"
	mr := NewDatagramReceiver(ch, socketFactory(socketPath, false), 1, 2)
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		mr.Run(ctx)
		wg.Done()
	}()

	// Wait until the socket is created
	tassert.Eventually(t, func() bool {
		return tassert.FileExists(t, socketPath)
	}, time.Second, 10*time.Millisecond)

	err := sendDataToSocket(socketPath, message)
	tassert.NoError(t, err)

	select {
	case d := <-ch:
		tassert.Len(t, d, 1)
		tassert.Equal(t, d[0].Msg, []byte(message))
		cancel()
	case <-time.After(time.Second):
		t.Errorf("Timeout, failed to read datagram")
		cancel()
	}
	wg.Wait()
}

func TestDatagramReceiver_UnixSocketIsRemovedOnContextCancellation(t *testing.T) {
	ch := make(chan []*Datagram, 1)

	socketPath := os.TempDir() + "/gostatsd_receiver_test_receive_uds.sock"
	mr := NewDatagramReceiver(ch, socketFactory(socketPath, false), 1, 2)
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		mr.Run(ctx)
		wg.Done()
	}()

	tassert.Eventually(t, func() bool {
		return tassert.FileExists(t, socketPath)
	}, time.Second, 10*time.Millisecond)
	cancel()
	wg.Wait()
	tassert.NoFileExists(t, socketPath)
}

func sendDataToSocket(socketPath string, data string) error {
	c, err := net.Dial("unixgram", socketPath)
	if err != nil {
		return err
	}
	defer c.Close()

	_, err = c.Write([]byte(data))
	return err
}
