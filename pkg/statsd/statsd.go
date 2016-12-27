package statsd

import (
	"context"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/atlassian/gostatsd"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

// DefaultBackends is the list of default backends' names.
var DefaultBackends = []string{"graphite"}

// DefaultMaxReaders is the default number of socket reading goroutines.
var DefaultMaxReaders = runtime.NumCPU()

// DefaultMaxWorkers is the default number of goroutines that aggregate metrics.
var DefaultMaxWorkers = runtime.NumCPU()

// DefaultPercentThreshold is the default list of applied percentiles.
var DefaultPercentThreshold = []float64{90}

// DefaultTags is the default list of additional tags.
var DefaultTags = gostatsd.Tags{}

const (
	// DefaultMaxCloudRequests is the maximum number of cloud provider requests per second.
	DefaultMaxCloudRequests = 40
	// DefaultBurstCloudRequests is the burst number of cloud provider requests per second.
	DefaultBurstCloudRequests = DefaultMaxCloudRequests + 20
	// DefaultExpiryInterval is the default expiry interval for metrics.
	DefaultExpiryInterval = 5 * time.Minute
	// DefaultFlushInterval is the default metrics flush interval.
	DefaultFlushInterval = 1 * time.Second
	// DefaultMetricsAddr is the default address on which to listen for metrics.
	DefaultMetricsAddr = ":8125"
	// DefaultMaxQueueSize is the default maximum number of buffered metrics per worker.
	DefaultMaxQueueSize = 10000 // arbitrary
	// DefaultMaxConcurrentEvents is the default maximum number of events sent concurrently.
	DefaultMaxConcurrentEvents = 1024 // arbitrary
)

const (
	// ParamBackends is the name of parameter with backends.
	ParamBackends = "backends"
	// ParamConsoleAddr is the name of parameter with console address.
	ParamConsoleAddr = "console-addr"
	// ParamCloudProvider is the name of parameter with the name of cloud provider.
	ParamCloudProvider = "cloud-provider"
	// ParamMaxCloudRequests is the name of parameter with maximum number of cloud provider requests per second.
	ParamMaxCloudRequests = "max-cloud-requests"
	// ParamBurstCloudRequests is the name of parameter with burst number of cloud provider requests per second.
	ParamBurstCloudRequests = "burst-cloud-requests"
	// ParamDefaultTags is the name of parameter with the list of additional tags.
	ParamDefaultTags = "default-tags"
	// ParamExpiryInterval is the name of parameter with expiry interval for metrics.
	ParamExpiryInterval = "expiry-interval"
	// ParamFlushInterval is the name of parameter with metrics flush interval.
	ParamFlushInterval = "flush-interval"
	// ParamMaxReaders is the name of parameter with number of socket readers.
	ParamMaxReaders = "max-readers"
	// ParamMaxWorkers is the name of parameter with number of goroutines that aggregate metrics.
	ParamMaxWorkers = "max-workers"
	// ParamMaxQueueSize is the name of parameter with maximum number of buffered metrics per worker.
	ParamMaxQueueSize = "max-queue-size"
	// ParamMaxConcurrentEvents is the name of parameter with maximum number of events sent concurrently.
	ParamMaxConcurrentEvents = "max-concurrent-events"
	// ParamMetricsAddr is the name of parameter with address on which to listen for metrics.
	ParamMetricsAddr = "metrics-addr"
	// ParamNamespace is the name of parameter with namespace for all metrics.
	ParamNamespace = "namespace"
	// ParamPercentThreshold is the name of parameter with list of applied percentiles.
	ParamPercentThreshold = "percent-threshold"
	// ParamWebAddr is the name of parameter with the address of the web-based console.
	ParamWebAddr = "web-addr"
)

// Server encapsulates all of the parameters necessary for starting up
// the statsd server. These can either be set via command line or directly.
type Server struct {
	Backends            []gostatsd.Backend
	ConsoleAddr         string
	CloudProvider       gostatsd.CloudProvider
	Limiter             *rate.Limiter
	DefaultTags         gostatsd.Tags
	ExpiryInterval      time.Duration
	FlushInterval       time.Duration
	MaxReaders          int
	MaxWorkers          int
	MaxQueueSize        int
	MaxConcurrentEvents int
	MaxEventQueueSize   int
	MetricsAddr         string
	Namespace           string
	PercentThreshold    []float64
	WebConsoleAddr      string
	Viper               *viper.Viper
}

// NewServer will create a new Server with the default configuration.
func NewServer() *Server {
	return &Server{
		ConsoleAddr:         DefaultConsoleAddr,
		Limiter:             rate.NewLimiter(DefaultMaxCloudRequests, DefaultBurstCloudRequests),
		DefaultTags:         DefaultTags,
		ExpiryInterval:      DefaultExpiryInterval,
		FlushInterval:       DefaultFlushInterval,
		MaxReaders:          DefaultMaxReaders,
		MaxWorkers:          DefaultMaxWorkers,
		MaxQueueSize:        DefaultMaxQueueSize,
		MaxConcurrentEvents: DefaultMaxConcurrentEvents,
		MetricsAddr:         DefaultMetricsAddr,
		PercentThreshold:    DefaultPercentThreshold,
		WebConsoleAddr:      DefaultWebConsoleAddr,
		Viper:               viper.New(),
	}
}

// AddFlags adds flags to the specified FlagSet.
func AddFlags(fs *pflag.FlagSet) {
	fs.String(ParamConsoleAddr, DefaultConsoleAddr, "If set, use as the address of the telnet-based console")
	fs.String(ParamCloudProvider, "", "If set, use the cloud provider to retrieve metadata about the sender")
	fs.Duration(ParamExpiryInterval, DefaultExpiryInterval, "After how long do we expire metrics (0 to disable)")
	fs.Duration(ParamFlushInterval, DefaultFlushInterval, "How often to flush metrics to the backends")
	fs.Int(ParamMaxReaders, DefaultMaxReaders, "Maximum number of socket readers")
	fs.Int(ParamMaxWorkers, DefaultMaxWorkers, "Maximum number of workers to process metrics")
	fs.Int(ParamMaxQueueSize, DefaultMaxQueueSize, "Maximum number of buffered metrics per worker")
	fs.Int(ParamMaxConcurrentEvents, DefaultMaxConcurrentEvents, "Maximum number of events sent concurrently")
	fs.String(ParamMetricsAddr, DefaultMetricsAddr, "Address on which to listen for metrics")
	fs.String(ParamNamespace, "", "Namespace all metrics")
	fs.String(ParamWebAddr, DefaultWebConsoleAddr, "If set, use as the address of the web-based console")
	//TODO Remove workaround when https://github.com/spf13/viper/issues/112 is fixed
	// https://github.com/spf13/viper/issues/200
	fs.String(ParamBackends, strings.Join(DefaultBackends, ","), "Comma-separated list of backends")
	fs.Int(ParamMaxCloudRequests, DefaultMaxCloudRequests, "Maximum number of cloud provider requests per second")
	fs.Int(ParamBurstCloudRequests, DefaultBurstCloudRequests, "Burst number of cloud provider requests per second")
	fs.String(ParamDefaultTags, strings.Join(DefaultTags, ","), "Comma-separated list of tags to add to all metrics")
	fs.String(ParamPercentThreshold, strings.Join(toStringSlice(DefaultPercentThreshold), ","), "Comma-separated list of percentiles")
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
			go func(b gostatsd.RunnableBackend) {
				defer wgBackends.Done()
				if err := b.Run(ctxBack); unexpectedErr(err) {
					log.Panicf("Backend %s quit unexpectedly: %v", b.Name(), err)
				}
			}(b)
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
	go func() {
		defer wgDispatcher.Done()
		if dispErr := dispatcher.Run(ctxDisp); unexpectedErr(dispErr) {
			log.Panicf("Dispatcher quit unexpectedly: %v", dispErr)
		}
	}()

	// 2. Start handlers
	ip := gostatsd.UnknownIP

	var handler Handler
	handler = NewDispatchingHandler(dispatcher, s.Backends, s.DefaultTags, uint(s.MaxConcurrentEvents))
	if s.CloudProvider != nil {
		ch := NewCloudHandler(s.CloudProvider, handler, s.Limiter, nil)
		handler = ch
		var wgCloudHandler sync.WaitGroup
		defer wgCloudHandler.Wait()                                           // Wait for handler to shutdown
		ctxHandler, cancelHandler := context.WithCancel(context.Background()) // Separate context!
		defer cancelHandler()                                                 // Tell the handler to shutdown
		wgCloudHandler.Add(1)
		go func() {
			defer wgCloudHandler.Done()
			if handlerErr := ch.Run(ctxHandler); unexpectedErr(handlerErr) {
				log.Panicf("Cloud handler quit unexpectedly: %v", handlerErr)
			}
		}()
		selfIP, err := s.CloudProvider.SelfIP()
		if err != nil {
			log.Warnf("Failed to get self ip: %v", err)
		} else {
			ip = selfIP
		}
	}

	// 3. Start the Receiver
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

	receiver := NewMetricReceiver(s.Namespace, handler)
	wgReceiver.Add(s.MaxReaders)
	for r := 0; r < s.MaxReaders; r++ {
		go func() {
			defer wgReceiver.Done()
			if e := receiver.Receive(ctx, c); unexpectedErr(e) {
				log.Panicf("Receiver quit unexpectedly: %v", e)
			}
		}()
	}

	// 4. Start the Flusher
	hostname := getHost()
	flusher := NewFlusher(s.FlushInterval, dispatcher, receiver, handler, s.Backends, ip, hostname)
	var wgFlusher sync.WaitGroup
	defer wgFlusher.Wait() // Wait for the Flusher to finish
	wgFlusher.Add(1)
	go func() {
		defer wgFlusher.Done()
		if err := flusher.Run(ctx); unexpectedErr(err) {
			log.Panicf("Flusher quit unexpectedly: %v", err)
		}
	}()

	// 5. Start the console(s)
	if s.ConsoleAddr != "" {
		console := ConsoleServer{s.ConsoleAddr, receiver, dispatcher, flusher}
		go console.ListenAndServe(ctx)
	}
	//if s.WebConsoleAddr != "" {
	//	console := WebConsoleServer{s.WebConsoleAddr, aggregator}
	//	go console.ListenAndServe()
	//}

	// 6. Send events on start and on stop
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
