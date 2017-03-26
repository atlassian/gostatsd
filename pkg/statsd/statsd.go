package statsd

import (
	"context"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/atlassian/gostatsd"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
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

	var handler Handler // nolint: gosimple
	handler = NewDispatchingHandler(dispatcher, s.Backends, s.DefaultTags, uint(s.MaxConcurrentEvents))
	if s.CloudProvider != nil {
		ch := NewCloudHandler(s.CloudProvider, handler, s.Limiter, &s.CacheOptions)
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
	flusher := NewMetricFlusher(s.FlushInterval, dispatcher, receiver, handler, s.Backends, ip, hostname)
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
