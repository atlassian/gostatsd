package statsd

import (
	"context"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/atlassian/gostatsd"
	stats "github.com/atlassian/gostatsd/pkg/statser"
	"github.com/jbenet/go-reuseport"

	"github.com/ash2k/stager"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

// Server encapsulates all of the parameters necessary for starting up
// the statsd server. These can either be set via command line or directly.
type Server struct {
	Backends            []gostatsd.Backend
	CloudProvider       gostatsd.CloudProvider
	Limiter             *rate.Limiter
	InternalTags        gostatsd.Tags
	InternalNamespace   string
	DefaultTags         gostatsd.Tags
	ExpiryInterval      time.Duration
	FlushInterval       time.Duration
	IgnoreHost          bool
	ConnPerReader       bool
	MaxReaders          int
	MaxParsers          int
	MaxWorkers          int
	MaxQueueSize        int
	MaxConcurrentEvents int
	MaxEventQueueSize   int
	MetricsAddr         string
	Namespace           string
	PercentThreshold    []float64
	HeartbeatInterval   time.Duration
	HeartbeatTags       gostatsd.Tags
	ReceiveBatchSize    int
	CacheOptions
	Viper *viper.Viper
}

// Run runs the server until context signals done.
func (s *Server) Run(ctx context.Context) error {
	return s.RunWithCustomSocket(ctx, socketFactory(s.MetricsAddr, s.ConnPerReader))
}

// SocketFactory is an indirection layer over net.ListenPacket() to allow for different implementations.
type SocketFactory func() (net.PacketConn, error)

func socketFactory(metricsAddr string, connPerReader bool) SocketFactory {
	if connPerReader {
		return func() (net.PacketConn, error) {
			return reuseport.ListenPacket("udp6", metricsAddr)
		}
	} else {
		conn, err := net.ListenPacket("udp", metricsAddr)
		return func() (net.PacketConn, error) {
			return conn, err
		}
	}
}

// RunWithCustomSocket runs the server until context signals done.
// Listening socket is created using sf.
func (s *Server) RunWithCustomSocket(ctx context.Context, sf SocketFactory) error {
	stgr := stager.New()
	defer stgr.Shutdown()
	// 0. Start runnable backends
	stage := stgr.NextStage()
	for _, b := range s.Backends {
		if b, ok := b.(gostatsd.RunnableBackend); ok {
			stage.StartWithContext(b.Run)
		}
	}

	// 1. Start the Dispatcher
	factory := agrFactory{
		percentThresholds: s.PercentThreshold,
		expiryInterval:    s.ExpiryInterval,
	}
	dispatcher := NewMetricDispatcher(s.MaxWorkers, s.MaxQueueSize, &factory)

	stage = stgr.NextStage()
	stage.StartWithContext(dispatcher.Run)

	// 2. Start handlers
	ip := gostatsd.UnknownIP

	var handler Handler // nolint: gosimple, megacheck
	handler = NewDispatchingHandler(dispatcher, s.Backends, s.DefaultTags, uint(s.MaxConcurrentEvents))
	if s.CloudProvider != nil {
		ch := NewCloudHandler(s.CloudProvider, handler, s.Limiter, &s.CacheOptions)
		handler = ch
		stage = stgr.NextStage()
		stage.StartWithContext(ch.Run)
		selfIP, err := s.CloudProvider.SelfIP()
		if err != nil {
			log.Warnf("Failed to get self ip: %v", err)
		} else {
			ip = selfIP
		}
	}

	// 3. Attach the dispatchers metrics
	hostname := getHost()
	namespace := s.Namespace
	if s.InternalNamespace != "" {
		if namespace != "" {
			namespace = namespace + "." + s.InternalNamespace
		} else {
			namespace = s.InternalNamespace
		}
	}

	bufferSize := 1000 // Estimating this is hard, and tends to cause loss under adverse conditions
	statser := stats.NewInternalStatser(bufferSize, s.InternalTags, namespace, hostname, handler)
	// TODO: Make internal metric dispatch configurable
	// statser := stats.NewLoggingStatser(s.InternalTags, log.NewEntry(log.New()))
	stage = stgr.NextStage()
	stage.StartWithContext(statser.Run)

	stage = stgr.NextStage()
	stage.StartWithContext(func(ctx context.Context) {
		dispatcher.RunMetrics(ctx, statser)
	})

	// 4. Start the heartbeat
	if s.HeartbeatInterval != 0 {
		hb := stats.NewHeartBeater(statser, "heartbeat", s.HeartbeatTags, s.HeartbeatInterval)
		stage = stgr.NextStage()
		stage.StartWithContext(hb.Run)
	}

	// 5. Start the Parser
	// Open receiver <-> parser chan
	datagrams := make(chan []*Datagram)

	parser := NewDatagramParser(datagrams, s.Namespace, s.IgnoreHost, handler, statser)
	stage = stgr.NextStage()
	stage.StartWithContext(parser.RunMetrics)
	for r := 0; r < s.MaxParsers; r++ {
		stage.StartWithContext(parser.Run)
	}

	// 6. Start the Receiver
	// Open socket
	c, err := sf()
	if err != nil {
		return err
	}
	defer func() {
		// This makes receivers error out and stop
		if e := c.Close(); e != nil {
			log.Warnf("Error closing socket: %v", e)
		}
	}()

	receiver := NewDatagramReceiver(datagrams, s.ReceiveBatchSize, statser)
	stage = stgr.NextStage()
	stage.StartWithContext(receiver.RunMetrics)
	for r := 0; r < s.MaxReaders; r++ {
		// Open socket
		c, err := sf()
		if err != nil {
			return err
		}
		defer func(c net.PacketConn) {
			// This makes receivers error out and stop
			if e := c.Close(); e != nil {
				log.Warnf("Error closing socket: %v", e)
			}
		}(c)

		stage.StartWithContext(func(ctx context.Context) {
			receiver.Receive(ctx, c)
		})
	}

	// 7. Start the Flusher
	flusher := NewMetricFlusher(s.FlushInterval, dispatcher, handler, s.Backends, ip, hostname, statser)
	stage = stgr.NextStage()
	stage.StartWithContext(flusher.Run)

	// 8. Send events on start and on stop
	// TODO: Push these in to statser
	defer sendStopEvent(handler, ip, hostname)
	sendStartEvent(ctx, handler, ip, hostname)

	// 9. Listen until done
	<-ctx.Done()
	return ctx.Err()
}

func sendStartEvent(ctx context.Context, handler Handler, selfIP gostatsd.IP, hostname string) {
	err := handler.DispatchEvent(ctx, &gostatsd.Event{
		Title:        "Gostatsd started",
		Text:         "Gostatsd started",
		DateHappened: time.Now().Unix(),
		Hostname:     hostname,
		SourceIP:     selfIP,
		Priority:     gostatsd.PriLow,
	})
	if unexpectedErr(err) {
		log.Warnf("Failed to send start event: %v", err)
	}
}

func sendStopEvent(handler Handler, selfIP gostatsd.IP, hostname string) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFunc()
	err := handler.DispatchEvent(ctx, &gostatsd.Event{
		Title:        "Gostatsd stopped",
		Text:         "Gostatsd stopped",
		DateHappened: time.Now().Unix(),
		Hostname:     hostname,
		SourceIP:     selfIP,
		Priority:     gostatsd.PriLow,
	})
	if unexpectedErr(err) {
		log.Warnf("Failed to send stop event: %v", err)
	}
	handler.WaitForEvents()
}

func getHost() string {
	host, err := os.Hostname()
	if err != nil {
		log.Warnf("Cannot get hostname: %v", err)
		return ""
	}
	return host
}

type agrFactory struct {
	percentThresholds []float64
	expiryInterval    time.Duration
}

func (af *agrFactory) Create() Aggregator {
	return NewMetricAggregator(af.percentThresholds, af.expiryInterval)
}

func toStringSlice(fs []float64) []string {
	s := make([]string, len(fs))
	for i, f := range fs {
		s[i] = strconv.FormatFloat(f, 'f', -1, 64)
	}
	return s
}

func unexpectedErr(err error) bool {
	return err != nil && err != context.Canceled && err != context.DeadlineExceeded
}
