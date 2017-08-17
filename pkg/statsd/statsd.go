package statsd

import (
	"context"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/statser"

	log "github.com/Sirupsen/logrus"
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
	MaxReaders          int
	MaxWorkers          int
	MaxQueueSize        int
	MaxConcurrentEvents int
	MaxEventQueueSize   int
	MetricsAddr         string
	Namespace           string
	PercentThreshold    []float64
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
	// 0. Start runnable backends
	var wgBackends sync.WaitGroup
	defer wgBackends.Wait()                                         // Wait for backends to shutdown
	ctxBack, cancelBack := context.WithCancel(context.Background()) // Separate context!
	defer cancelBack()                                              // Tell backends to shutdown
	for _, b := range s.Backends {
		if b, ok := b.(gostatsd.RunnableBackend); ok {
			wgBackends.Add(1)
			go b.Run(ctxBack, wgBackends.Done)
		}
	}

	// 1. Start the Dispatcher
	factory := agrFactory{
		percentThresholds: s.PercentThreshold,
		expiryInterval:    s.ExpiryInterval,
	}
	dispatcher := NewMetricDispatcher(s.MaxWorkers, s.MaxQueueSize, &factory)

	var wgDispatcher sync.WaitGroup
	defer wgDispatcher.Wait()                                       // Wait for dispatcher to shutdown
	ctxDisp, cancelDisp := context.WithCancel(context.Background()) // Separate context!
	defer cancelDisp()                                              // Tell the dispatcher to shutdown
	wgDispatcher.Add(1)
	go dispatcher.Run(ctxDisp, wgDispatcher.Done)

	// 2. Start handlers
	ip := gostatsd.UnknownIP

	var handler Handler // nolint: gosimple, megacheck
	handler = NewDispatchingHandler(dispatcher, s.Backends, s.DefaultTags, uint(s.MaxConcurrentEvents))
	if s.CloudProvider != nil {
		ch := NewCloudHandler(s.CloudProvider, handler, s.Limiter, &s.CacheOptions)
		handler = ch
		var wgCloudHandler sync.WaitGroup
		defer wgCloudHandler.Wait()                                           // Wait for handler to shutdown
		ctxHandler, cancelHandler := context.WithCancel(context.Background()) // Separate context!
		defer cancelHandler()                                                 // Tell the handler to shutdown
		wgCloudHandler.Add(1)
		go ch.Run(ctxHandler, wgCloudHandler.Done)
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

	var wgStatser sync.WaitGroup
	defer wgStatser.Wait()

	ctxStatser, cancelStatser := context.WithCancel(context.Background())
	defer cancelStatser()
	statser := statser.NewInternalStatser(ctxStatser, &wgStatser, s.InternalTags, namespace, hostname, handler)
	// TODO: Make internal metric dispatch configurable
	// statser := NewLoggingStatser(s.InternalTags, log.NewEntry(log.New()))
	dispatcher.runMetrics(ctxStatser, statser)

	// 4. Start the Receiver
	var wgReceiver sync.WaitGroup
	defer wgReceiver.Wait() // Wait for all receivers to finish

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

	receiver := NewMetricReceiver(s.Namespace, s.IgnoreHost, handler, statser)
	wgReceiver.Add(s.MaxReaders + 1)
	go receiver.runMetrics(ctx, wgReceiver.Done)
	for r := 0; r < s.MaxReaders; r++ {
		go receiver.Receive(ctx, wgReceiver.Done, c)
	}

	// 5. Start the Flusher
	flusher := NewMetricFlusher(s.FlushInterval, dispatcher, handler, s.Backends, ip, hostname, statser)
	var wgFlusher sync.WaitGroup
	defer wgFlusher.Wait() // Wait for the Flusher to finish
	wgFlusher.Add(1)
	go flusher.Run(ctx, wgFlusher.Done)

	// 6. Send events on start and on stop
	// TODO: Push these in to statser
	defer sendStopEvent(handler, ip, hostname)
	sendStartEvent(ctx, handler, ip, hostname)

	// 7. Listen until done
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
