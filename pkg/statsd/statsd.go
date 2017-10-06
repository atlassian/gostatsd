package statsd

import (
	"context"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/atlassian/gostatsd"
	stats "github.com/atlassian/gostatsd/pkg/statser"

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
	MaxReaders          int
	MaxParsers          int
	MaxWorkers          int
	MaxQueueSize        int
	MaxConcurrentEvents int
	MaxEventQueueSize   int
	MetricsAddr         string
	Namespace           string
	PercentThreshold    []float64
	IgnoreHost          bool
	HeartbeatEnabled    bool
	HeartbeatTags       gostatsd.Tags
	ReceiveBatchSize    int
	CacheOptions
	Viper *viper.Viper
}

// Run runs the server until context signals done.
func (s *Server) Run(ctx context.Context) error {
	return s.RunWithCustomSocket(ctx, func() (net.PacketConn, error) {
		return net.ListenPacket("udp", s.MetricsAddr)
	})
}

// SocketFactory is an indirection layer over net.ListenPacket() to allow for different implementations.
type SocketFactory func() (net.PacketConn, error)

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

	// 1. Start the backend handler
	factory := agrFactory{
		percentThresholds: s.PercentThreshold,
		expiryInterval:    s.ExpiryInterval,
	}

	backendHandler := NewBackendHandler(s.Backends, uint(s.MaxConcurrentEvents), s.MaxWorkers, s.MaxQueueSize, &factory)
	metrics := MetricHandler(backendHandler)
	events := EventHandler(backendHandler)

	stage = stgr.NextStage()
	stage.StartWithContext(backendHandler.Run)

	// 2. Start the default tag adder
	th := NewTagHandler(metrics, events, s.DefaultTags)
	metrics = th
	events = th

	// 3. Start the cloud handler
	ip := gostatsd.UnknownIP
	if s.CloudProvider != nil {
		ch := NewCloudHandler(s.CloudProvider, metrics, events, s.Limiter, &s.CacheOptions)
		metrics = ch
		events = ch
		stage = stgr.NextStage()
		stage.StartWithContext(ch.Run)
		selfIP, err := s.CloudProvider.SelfIP()
		if err != nil {
			log.Warnf("Failed to get self ip: %v", err)
		} else {
			ip = selfIP
		}
	}

	// 4. Create the statser
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
	statser := stats.NewInternalStatser(bufferSize, s.InternalTags, namespace, hostname, metrics, events)
	// TODO: Make internal metric dispatch configurable
	// statser := stats.NewLoggingStatser(s.InternalTags, log.NewEntry(log.New()))
	stage = stgr.NextStage()
	stage.StartWithContext(statser.Run)

	// 5. Attach the statser to anything that needs it
	stage = stgr.NextStage()
	stage.StartWithContext(func(ctx context.Context) {
		backendHandler.RunMetrics(ctx, statser)
	})
	for _, backend := range s.Backends {
		if metricEmitter, ok := backend.(MetricEmitter); ok {
			stage.StartWithContext(func(ctx context.Context) {
				metricEmitter.RunMetrics(ctx, statser)
			})
		}
	}

	// 6. Start the heartbeat
	if s.HeartbeatEnabled {
		hb := stats.NewHeartBeater(statser, "heartbeat", s.HeartbeatTags)
		stage = stgr.NextStage()
		stage.StartWithContext(hb.Run)
	}

	// 7. Start the Parser
	// Open receiver <-> parser chan
	datagrams := make(chan []*Datagram)

	parser := NewDatagramParser(datagrams, s.Namespace, s.IgnoreHost, metrics, events, statser)
	stage = stgr.NextStage()
	stage.StartWithContext(parser.RunMetrics)
	for r := 0; r < s.MaxParsers; r++ {
		stage.StartWithContext(parser.Run)
	}

	// 8. Start the Receiver
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
		stage.StartWithContext(func(ctx context.Context) {
			receiver.Receive(ctx, c)
		})
	}

	// 9. Start the Flusher
	flusher := NewMetricFlusher(s.FlushInterval, backendHandler, s.Backends, ip, hostname, statser)
	stage = stgr.NextStage()
	stage.StartWithContext(flusher.Run)

	// 10. Send events on start and on stop
	// TODO: Push these in to statser
	defer sendStopEvent(events, ip, hostname)
	sendStartEvent(ctx, events, ip, hostname)

	// 11. Listen until done
	<-ctx.Done()
	return ctx.Err()
}

func sendStartEvent(ctx context.Context, events EventHandler, selfIP gostatsd.IP, hostname string) {
	err := events.DispatchEvent(ctx, &gostatsd.Event{
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

func sendStopEvent(events EventHandler, selfIP gostatsd.IP, hostname string) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFunc()
	err := events.DispatchEvent(ctx, &gostatsd.Event{
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
	events.WaitForEvents()
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
