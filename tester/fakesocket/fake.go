package fakesocket

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
)

// FakeMetric is a fake metric.
var FakeMetric = []byte("foo.bar.baz:2|c")

// FakeAddr is a fake net.Addr
type FakeAddr struct{}

// Network dummy impl.
func (fa FakeAddr) Network() string { return "udp" }

// String dummy impl.
func (fa FakeAddr) String() string { return "127.0.0.1:8181" }

// FakePacketConn is a fake net.PacketConn providing FakeMetric when read from.
type FakePacketConn struct{}

// ReadFrom copies FakeMetric into b.
func (fpc FakePacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n := copy(b, FakeMetric)
	return n, FakeAddr{}, nil
}

// WriteTo dummy impl.
func (fpc FakePacketConn) WriteTo(b []byte, addr net.Addr) (int, error) { return 0, nil }

// Close dummy impl.
func (fpc FakePacketConn) Close() error { return nil }

// LocalAddr dummy impl.
func (fpc FakePacketConn) LocalAddr() net.Addr { return FakeAddr{} }

// SetDeadline dummy impl.
func (fpc FakePacketConn) SetDeadline(t time.Time) error { return nil }

// SetReadDeadline dummy impl.
func (fpc FakePacketConn) SetReadDeadline(t time.Time) error { return nil }

// SetWriteDeadline dummy impl.
func (fpc FakePacketConn) SetWriteDeadline(t time.Time) error { return nil }

// FakeRandomPacketConn is a fake net.PacketConn providing random fake metrics.
type FakeRandomPacketConn struct {
	FakePacketConn
}

// ReadFrom generates random metric and writes in into b.
func (frpc FakeRandomPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	num := rand.Int31n(10000) // Randomize metric name
	buf := new(bytes.Buffer)
	switch rand.Int31n(4) {
	case 0: // Counter
		fmt.Fprintf(buf, "statsd.tester.counter_%d:%f|c\n", num, rand.Float64()*100)
	case 1: // Gauge
		fmt.Fprintf(buf, "statsd.tester.gauge_%d:%f|g\n", num, rand.Float64()*100)
	case 2: // Timer
		n := rand.Intn(9) + 1
		for i := 0; i < n; i++ {
			fmt.Fprintf(buf, "statsd.tester.timer_%d:%f|ms\n", num, rand.Float64()*100)
		}
	case 3: // Set
		for i := 0; i < 100; i++ {
			fmt.Fprintf(buf, "statsd.tester.set_%d:%d|s\n", num, rand.Int31n(9)+1)
		}
	default:
		panic(errors.New("unreachable"))
	}
	n := copy(b, buf.Bytes())
	return n, FakeAddr{}, nil
}

// CountingFakeRandomPacketConn is a fake net.PacketConn providing random fake metrics and counting number of performed read operations.
// Safe for concurrent use.
type CountingFakeRandomPacketConn struct {
	NumReads uint64
	FakeRandomPacketConn
}

// ReadFrom generates random metric and writes in into b.
func (frpc *CountingFakeRandomPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	atomic.AddUint64(&frpc.NumReads, 1)
	return frpc.FakeRandomPacketConn.ReadFrom(b)
}

// Factory is a replacement for net.ListenPacket() that produces instances of FakeRandomPacketConn.
func Factory() (net.PacketConn, error) {
	return FakeRandomPacketConn{}, nil
}
