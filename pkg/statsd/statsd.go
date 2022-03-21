package statsd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/ash2k/stager"
	"github.com/libp2p/go-reuseport"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"
	"github.com/atlassian/gostatsd/pkg/transport"
	"github.com/atlassian/gostatsd/pkg/web"
)

// Server encapsulates all of the parameters necessary for starting up
// the statsd server. These can either be set via command line or directly.
type Server struct {
	Runnables                 []gostatsd.Runnable
	Backends                  []gostatsd.Backend
	CachedInstances           gostatsd.CachedInstances
	InternalTags              gostatsd.Tags
	InternalNamespace         string
	DefaultTags               gostatsd.Tags
	ExpiryIntervalCounter     time.Duration
	ExpiryIntervalGauge       time.Duration
	ExpiryIntervalSet         time.Duration
	ExpiryIntervalTimer       time.Duration
	FlushInterval             time.Duration
	FlushOffset               time.Duration
	FlushAligned              bool
	MaxReaders                int
	MaxParsers                int
	MaxWorkers                int
	MaxQueueSize              int
	MaxConcurrentEvents       int
	MaxEventQueueSize         int
	EstimatedTags             int
	MetricsAddr               string
	Namespace                 string
	StatserType               string
	PercentThreshold          []float64
	IgnoreHost                bool
	ConnPerReader             bool
	HeartbeatEnabled          bool
	HeartbeatTags             gostatsd.Tags
	ReceiveBatchSize          int
	DisabledSubTypes          gostatsd.TimerSubtypes
	HistogramLimit            uint32
	BadLineRateLimitPerSecond rate.Limit
	ServerMode                string
	Hostname                  gostatsd.Source
	LogRawMetric              bool
	Viper                     *viper.Viper
	TransportPool             *transport.TransportPool
}

// Run runs the server until context signals done.
func (s *Server) Run(ctx context.Context) error {
	return s.RunWithCustomSocket(ctx, socketFactory(s.MetricsAddr, s.ConnPerReader))
}

// SocketFactory is an indirection layer over net.ListenPacket() to allow for different implementations.
type SocketFactory func() (net.PacketConn, error)

func socketFactory(metricsAddr string, connPerReader bool) SocketFactory {
	if connPerReader {
		// go-reuseport requires explicitly representing the unspecified address
		addr, err := net.ResolveUDPAddr("udp", metricsAddr)
		if err != nil {
			// let it fall through and be caught later
		} else if addr.IP.Equal(net.IP{}) {
			metricsAddr = fmt.Sprintf("[%s]%s", net.IPv6unspecified, metricsAddr)
		}
		return func() (net.PacketConn, error) {
			return reuseport.ListenPacket("udp", metricsAddr)
		}
	}

	conn, err := net.ListenPacket(networkFromAddress(metricsAddr), metricsAddr)
	return func() (net.PacketConn, error) {
		return conn, err
	}
}

//networkFromAddress returns the network type based on the provided address
//if the address starts with a slash (unix absolute path) it will be considered a unix socket
//otherwise it will default to UDP
func networkFromAddress(addr string) string {
	if len(addr) > 0 && addr[0:1] == "/" {
		return "unixgram"
	}
	return "udp"
}

func (s *Server) createStandaloneSink() (gostatsd.PipelineHandler, []gostatsd.Runnable, error) {
	var runnables []gostatsd.Runnable

	// Create the backend handler
	factory := agrFactory{
		percentThresholds:     s.PercentThreshold,
		expiryIntervalCounter: s.ExpiryIntervalCounter,
		expiryIntervalGauge:   s.ExpiryIntervalGauge,
		expiryIntervalSet:     s.ExpiryIntervalSet,
		expiryIntervalTimer:   s.ExpiryIntervalTimer,
		disabledSubtypes:      s.DisabledSubTypes,
		histogramLimit:        s.HistogramLimit,
	}

	backendHandler := NewBackendHandler(s.Backends, uint(s.MaxConcurrentEvents), s.MaxWorkers, s.MaxQueueSize, &factory)
	runnables = append(runnables, backendHandler.Run, backendHandler.RunMetricsContext)

	// Create the Flusher
	flusher := NewMetricFlusher(s.FlushInterval, s.FlushOffset, s.FlushAligned, backendHandler, s.Backends)
	runnables = append(runnables, flusher.Run)

	return backendHandler, runnables, nil
}

func (s *Server) createForwarderSink(logger logrus.FieldLogger) (gostatsd.PipelineHandler, []gostatsd.Runnable, error) {
	forwarderHandler, err := NewHttpForwarderHandlerV2FromViper(
		logger,
		s.Viper,
		s.TransportPool,
	)
	if err != nil {
		return nil, nil, err
	}

	// Create a Flusher, this is primarily for all the periodic metrics which are emitted.
	flusher := NewMetricFlusher(s.FlushInterval, 0, false, nil, s.Backends)

	return forwarderHandler, []gostatsd.Runnable{forwarderHandler.Run, forwarderHandler.RunMetricsContext, flusher.Run}, nil
}

func (s *Server) createFinalSink(logger logrus.FieldLogger) (gostatsd.PipelineHandler, []gostatsd.Runnable, error) {
	if s.ServerMode == "standalone" {
		return s.createStandaloneSink()
	} else if s.ServerMode == "forwarder" {
		return s.createForwarderSink(logger)
	}
	return nil, nil, errors.New("invalid server-mode, must be standalone, or forwarder")
}

// RunWithCustomSocket runs the server until context signals done.
// Listening socket is created using sf.
func (s *Server) RunWithCustomSocket(ctx context.Context, sf SocketFactory) error {
	logger := logrus.StandardLogger()

	handler, runnables, err := s.createFinalSink(logger)
	if err != nil {
		return err
	}

	runnables = append(append(make([]gostatsd.Runnable, 0, len(s.Runnables)), s.Runnables...), runnables...)

	// Create the tag processor
	handler = NewTagHandlerFromViper(s.Viper, handler, s.DefaultTags)

	// Create the cloud handler
	if s.CachedInstances != nil {
		cloudHandler := NewCloudHandler(s.CachedInstances, handler)
		runnables = gostatsd.MaybeAppendRunnable(runnables, cloudHandler)
		handler = cloudHandler
	}

	// Create the heartbeater
	if s.HeartbeatEnabled {
		hb := stats.NewHeartBeater("heartbeat", s.HeartbeatTags)
		runnables = gostatsd.MaybeAppendRunnable(runnables, hb)
	}

	// Open receiver <-> parser chan
	datagrams := make(chan []*Datagram)

	// Create the Parser
	parser := NewDatagramParser(datagrams, s.Namespace, s.IgnoreHost, s.EstimatedTags, handler, s.BadLineRateLimitPerSecond, s.LogRawMetric, logger)
	runnables = append(runnables, parser.RunMetricsContext)
	for i := 0; i < s.MaxParsers; i++ {
		runnables = append(runnables, parser.Run)
	}

	// Create the Receiver
	receiver := NewDatagramReceiver(datagrams, sf, s.MaxReaders, s.ReceiveBatchSize)
	runnables = gostatsd.MaybeAppendRunnable(runnables, receiver)

	// Create the Statser
	hostname := s.Hostname
	statser := s.createStatser(hostname, handler, logger)
	runnables = gostatsd.MaybeAppendRunnable(runnables, statser)

	// Create any http servers
	httpServers, err := web.NewHttpServersFromViper(s.Viper, logger, handler)
	if err != nil {
		return err
	}
	for _, server := range httpServers {
		runnables = gostatsd.MaybeAppendRunnable(runnables, server)
	}

	// Start the world!
	runCtx := stats.NewContext(context.Background(), statser)
	stgr := stager.New()
	defer stgr.Shutdown()
	for _, runnable := range runnables {
		stgr.NextStageWithContext(runCtx).StartWithContext(runnable)
	}

	// sendStopEvent uses its own context with a timeout, because the system is shutting down
	defer sendStopEvent(statser, hostname)
	sendStartEvent(runCtx, statser, hostname)

	// Listen until done
	<-ctx.Done()
	return ctx.Err()
}

func (s *Server) createStatser(hostname gostatsd.Source, handler gostatsd.PipelineHandler, logger logrus.FieldLogger) stats.Statser {
	switch s.StatserType {
	case gostatsd.StatserNull:
		return stats.NewNullStatser()
	case gostatsd.StatserLogging:
		return stats.NewLoggingStatser(s.InternalTags, logger)
	default:
		namespace := s.Namespace
		if s.InternalNamespace != "" {
			if namespace != "" {
				namespace = namespace + "." + s.InternalNamespace
			} else {
				namespace = s.InternalNamespace
			}
		}
		return stats.NewInternalStatser(s.InternalTags, namespace, hostname, handler)
	}
}

func sendStartEvent(ctx context.Context, statser stats.Statser, hostname gostatsd.Source) {
	statser.Event(ctx, &gostatsd.Event{
		Title:        "Gostatsd started",
		Text:         "Gostatsd started",
		DateHappened: time.Now().Unix(),
		Source:       hostname,
		Priority:     gostatsd.PriLow,
	})
}

func sendStopEvent(statser stats.Statser, hostname gostatsd.Source) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFunc()
	statser.Event(ctx, &gostatsd.Event{
		Title:        "Gostatsd stopped",
		Text:         "Gostatsd stopped",
		DateHappened: time.Now().Unix(),
		Source:       hostname,
		Priority:     gostatsd.PriLow,
	})
	statser.WaitForEvents()
}

type agrFactory struct {
	percentThresholds     []float64
	expiryIntervalCounter time.Duration
	expiryIntervalGauge   time.Duration
	expiryIntervalSet     time.Duration
	expiryIntervalTimer   time.Duration
	disabledSubtypes      gostatsd.TimerSubtypes
	histogramLimit        uint32
}

func (af *agrFactory) Create() Aggregator {
	return NewMetricAggregator(
		af.percentThresholds,
		af.expiryIntervalCounter,
		af.expiryIntervalGauge,
		af.expiryIntervalSet,
		af.expiryIntervalTimer,
		af.disabledSubtypes,
		af.histogramLimit,
	)
}
