package statsd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"
	"github.com/atlassian/gostatsd/pkg/transport"
	"github.com/atlassian/gostatsd/pkg/web"

	"github.com/ash2k/stager"
	"github.com/libp2p/go-reuseport"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

// Server encapsulates all of the parameters necessary for starting up
// the statsd server. These can either be set via command line or directly.
type Server struct {
	Backends                  []gostatsd.Backend
	CloudHandlerFactory       *CloudHandlerFactory
	InternalTags              gostatsd.Tags
	InternalNamespace         string
	DefaultTags               gostatsd.Tags
	ExpiryInterval            time.Duration
	FlushInterval             time.Duration
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
	Hostname                  string
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
	} else {
		conn, err := net.ListenPacket("udp", metricsAddr)
		return func() (net.PacketConn, error) {
			return conn, err
		}
	}
}

func (s *Server) createStandaloneSink() (gostatsd.PipelineHandler, []gostatsd.Runnable, error) {
	var runnables []gostatsd.Runnable

	for _, backend := range s.Backends {
		if r, ok := backend.(gostatsd.Runner); ok {
			runnables = append(runnables, r.Run)
		}
	}

	// Create the backend handler
	factory := agrFactory{
		percentThresholds: s.PercentThreshold,
		expiryInterval:    s.ExpiryInterval,
		disabledSubtypes:  s.DisabledSubTypes,
		histogramLimit:    s.HistogramLimit,
	}

	backendHandler := NewBackendHandler(s.Backends, uint(s.MaxConcurrentEvents), s.MaxWorkers, s.MaxQueueSize, &factory)
	runnables = append(runnables, backendHandler.Run, backendHandler.RunMetricsContext)

	// Create the Flusher
	flusher := NewMetricFlusher(s.FlushInterval, backendHandler, s.Backends)
	runnables = append(runnables, flusher.Run)

	return backendHandler, runnables, nil
}

func (s *Server) createForwarderSink() (gostatsd.PipelineHandler, []gostatsd.Runnable, error) {
	forwarderHandler, err := NewHttpForwarderHandlerV2FromViper(
		log.StandardLogger(),
		s.Viper,
		s.TransportPool,
	)
	if err != nil {
		return nil, nil, err
	}

	// Create a Flusher, this is primarily for all the periodic metrics which are emitted.
	flusher := NewMetricFlusher(s.FlushInterval, nil, s.Backends)

	return forwarderHandler, []gostatsd.Runnable{forwarderHandler.Run, forwarderHandler.RunMetrics, flusher.Run}, nil
}

func (s *Server) createFinalSink() (gostatsd.PipelineHandler, []gostatsd.Runnable, error) {
	if s.ServerMode == "standalone" {
		return s.createStandaloneSink()
	} else if s.ServerMode == "forwarder" {
		return s.createForwarderSink()
	}
	return nil, nil, errors.New("invalid server-mode, must be standalone, or forwarder")
}

// RunWithCustomSocket runs the server until context signals done.
// Listening socket is created using sf.
func (s *Server) RunWithCustomSocket(ctx context.Context, sf SocketFactory) error {
	handler, runnables, err := s.createFinalSink()
	if err != nil {
		return err
	}

	// Create the tag processor
	handler = NewTagHandlerFromViper(s.Viper, handler, s.DefaultTags)

	// Create the cloud handler
	ip := gostatsd.UnknownIP
	if s.CloudHandlerFactory != nil {
		cloudHandler := s.CloudHandlerFactory.NewCloudHandler(handler)
		runnables = append(runnables, cloudHandler.Run)
		handler = cloudHandler
		selfIP, err2 := cloudHandler.cloud.SelfIP()
		if err2 != nil {
			log.Warnf("Failed to get self ip: %v", err2)
		} else {
			ip = selfIP
		}
	}

	// Create the heartbeater
	if s.HeartbeatEnabled {
		hb := stats.NewHeartBeater("heartbeat", s.HeartbeatTags)
		runnables = append(runnables, hb.Run)
	}

	// Open receiver <-> parser chan
	datagrams := make(chan []*Datagram)

	// Create the Parser
	parser := NewDatagramParser(datagrams, s.Namespace, s.IgnoreHost, s.EstimatedTags, handler, s.BadLineRateLimitPerSecond, s.LogRawMetric)
	runnables = append(runnables, parser.RunMetrics)
	for i := 0; i < s.MaxParsers; i++ {
		runnables = append(runnables, parser.Run)
	}

	// Create the Receiver
	receiver := NewDatagramReceiver(datagrams, sf, s.MaxReaders, s.ReceiveBatchSize)
	runnables = append(runnables, receiver.RunMetrics)
	runnables = append(runnables, receiver.Run) // loop is contained in Run to keep additional logic contained

	// Create the Statser
	hostname := s.Hostname
	statser := s.createStatser(hostname, handler)
	if runner, ok := statser.(gostatsd.Runner); ok {
		runnables = append(runnables, runner.Run)
	}

	// Create any http servers
	httpServers, err := web.NewHttpServersFromViper(s.Viper, log.StandardLogger(), handler)
	if err != nil {
		return err
	}
	for _, server := range httpServers {
		runnables = append(runnables, server.Run)
	}

	// Start the world!
	runCtx := stats.NewContext(context.Background(), statser)
	stgr := stager.New()
	defer stgr.Shutdown()
	for _, runnable := range runnables {
		stgr.NextStageWithContext(runCtx).StartWithContext(runnable)
	}

	// Send events on start and on stop
	// TODO: Push these in to statser
	defer sendStopEvent(handler, ip, hostname)
	sendStartEvent(runCtx, handler, ip, hostname)

	// Listen until done
	<-ctx.Done()
	return ctx.Err()
}

func (s *Server) createStatser(hostname string, handler gostatsd.PipelineHandler) stats.Statser {
	switch s.StatserType {
	case StatserNull:
		return stats.NewNullStatser()
	case StatserLogging:
		return stats.NewLoggingStatser(s.InternalTags, log.NewEntry(log.New()))
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

func sendStartEvent(ctx context.Context, handler gostatsd.PipelineHandler, selfIP gostatsd.IP, hostname string) {
	handler.DispatchEvent(ctx, &gostatsd.Event{
		Title:        "Gostatsd started",
		Text:         "Gostatsd started",
		DateHappened: time.Now().Unix(),
		Hostname:     hostname,
		SourceIP:     selfIP,
		Priority:     gostatsd.PriLow,
	})
}

func sendStopEvent(handler gostatsd.PipelineHandler, selfIP gostatsd.IP, hostname string) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFunc()
	handler.DispatchEvent(ctx, &gostatsd.Event{
		Title:        "Gostatsd stopped",
		Text:         "Gostatsd stopped",
		DateHappened: time.Now().Unix(),
		Hostname:     hostname,
		SourceIP:     selfIP,
		Priority:     gostatsd.PriLow,
	})
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
	disabledSubtypes  gostatsd.TimerSubtypes
	histogramLimit    uint32
}

func (af *agrFactory) Create() Aggregator {
	return NewMetricAggregator(af.percentThresholds, af.expiryInterval, af.disabledSubtypes, af.histogramLimit)
}

func toStringSlice(fs []float64) []string {
	s := make([]string, len(fs))
	for i, f := range fs {
		s[i] = strconv.FormatFloat(f, 'f', -1, 64)
	}
	return s
}
