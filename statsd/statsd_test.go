package statsd

import (
	"net"
	"testing"
	"time"

	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

// TestStatsdThroughput emulates statsd work using fake network socket and null backend to
// measure throughput.
func TestStatsdThroughput(t *testing.T) {
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
	return fakePacketConn{}, nil
}
