package statsd

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"

	"github.com/ash2k/stager"
	"github.com/jbenet/go-reuseport"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

// Server encapsulates all of the parameters necessary for starting up
// the statsd server. These can either be set via command line or directly.
type Server struct {
	Backends                  []gostatsd.Backend
	CloudProvider             gostatsd.CloudProvider
	Limiter                   *rate.Limiter
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
	BadLineRateLimitPerSecond rate.Limit
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

// RunWithCustomSocket runs the server until context signals done.
// Listening socket is created using sf.
func (s *Server) RunWithCustomSocket(ctx context.Context, sf SocketFactory) error {
	var runnables []gostatsd.Runnable
	var handler gostatsd.PipelineHandler

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
	}

	backendHandler := NewBackendHandler(s.Backends, uint(s.MaxConcurrentEvents), s.MaxWorkers, s.MaxQueueSize, &factory)
	handler = backendHandler
	runnables = append(runnables, backendHandler.Run)

	// Create the tag processor
	handler = NewTagHandlerFromViper(s.Viper, handler, s.DefaultTags)

	// Create the cloud handler
	ip := gostatsd.UnknownIP
	if s.CloudProvider != nil {
		cloudHandler := NewCloudHandler(s.CloudProvider, handler, log.StandardLogger(), s.Limiter, &s.CacheOptions)
		runnables = append(runnables, cloudHandler.Run)
		handler = cloudHandler
		selfIP, err := s.CloudProvider.SelfIP()
		if err != nil {
			log.Warnf("Failed to get self ip: %v", err)
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
	parser := NewDatagramParser(datagrams, s.Namespace, s.IgnoreHost, s.EstimatedTags, handler, s.BadLineRateLimitPerSecond)
	runnables = append(runnables, parser.RunMetrics)
	for i := 0; i < s.MaxParsers; i++ {
		runnables = append(runnables, parser.Run)
	}

	// Create the Receiver
	receiver := NewDatagramReceiver(datagrams, sf, s.MaxReaders, s.ReceiveBatchSize)
	runnables = append(runnables, receiver.RunMetrics)
	runnables = append(runnables, receiver.Run) // loop is contained in Run to keep additional logic contained

	// Create the Flusher
	flusher := NewMetricFlusher(s.FlushInterval, backendHandler, s.Backends)
	runnables = append(runnables, flusher.Run)

	// Create the Statser
	hostname := getHost()
	statser := s.createStatser(hostname, handler)
	if runner, ok := statser.(gostatsd.Runner); ok {
		runnables = append(runnables, runner.Run)
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
}

func (af *agrFactory) Create() Aggregator {
	return NewMetricAggregator(af.percentThresholds, af.expiryInterval, af.disabledSubtypes)
}

func toStringSlice(fs []float64) []string {
	s := make([]string, len(fs))
	for i, f := range fs {
		s[i] = strconv.FormatFloat(f, 'f', -1, 64)
	}
	return s
}
