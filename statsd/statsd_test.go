package statsd

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

// TestStatsdThroughput emulates statsd work using fake network socket and null backend to
// measure throughput.
func TestStatsdThroughput(t *testing.T) {
	rand.Seed(time.Now().Unix())
	s := Server{
		Backends:         []string{"null"},
		ConsoleAddr:      "",
		DefaultTags:      DefaultTags,
		ExpiryInterval:   DefaultExpiryInterval,
		FlushInterval:    DefaultFlushInterval,
		MaxReaders:       DefaultMaxReaders,
		MaxWorkers:       DefaultMaxWorkers,
		MaxQueueSize:     DefaultMaxQueueSize,
		PercentThreshold: DefaultPercentThreshold,
		Viper:            viper.New(),
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	err := s.runWithCustomSocket(ctx, fakeSocketFactory)
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		t.Errorf("statsd run failed: %v", err)
	}
}

func fakeSocketFactory() (net.PacketConn, error) {
	return fakeRandomPacketConn{}, nil
}

type fakeRandomPacketConn struct {
	fakePacketConn
}

func (frpc fakeRandomPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
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
	return n, fakeAddr{}, nil
}
