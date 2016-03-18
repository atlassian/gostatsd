package statsd

import (
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/jtblin/gostatsd/backend"
	_ "github.com/jtblin/gostatsd/backend/backends" // import backends for initialisation
	"github.com/jtblin/gostatsd/cloudprovider"
	_ "github.com/jtblin/gostatsd/cloudprovider/providers" // import cloud providers for initialisation
	"github.com/jtblin/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Server encapsulates all of the parameters necessary for starting up
// the statsd server. These can either be set via command line or directly.
type Server struct {
	aggregator       *MetricAggregator
	Backends         []string
	ConfigPath       string
	ConsoleAddr      string
	CloudProvider    string
	CPUProfile       string
	DefaultTags      []string
	ExpiryInterval   time.Duration
	FlushInterval    time.Duration
	MaxReaders       int
	MaxWorkers       int
	MaxMessengers    int
	MetricsAddr      string
	Namespace        string
	PercentThreshold []string
	Verbose          bool
	Version          bool
	WebConsoleAddr   string
}

var server *Server

// NewServer will create a new Server with default values if none exists
// otherwise it will return the singleton.
func NewServer() *Server {
	if server != nil {
		return server
	}
	server = &Server{
		Backends:         []string{"graphite"},
		ConsoleAddr:      ":8126",
		ExpiryInterval:   5 * time.Minute,
		FlushInterval:    1 * time.Second,
		MaxReaders:       runtime.NumCPU(),
		MaxMessengers:    runtime.NumCPU() * 8,
		MaxWorkers:       runtime.NumCPU() * 8 * 8,
		MetricsAddr:      ":8125",
		PercentThreshold: []string{"90"},
		WebConsoleAddr:   ":8181",
	}
	return server
}

// AddFlags adds flags for a specific DockerAuthServer to the specified FlagSet
func (s *Server) AddFlags(fs *pflag.FlagSet) {
	fs.StringSliceVar(&s.Backends, "backends", s.Backends, "Comma-separated list of backends")
	fs.StringVar(&s.ConfigPath, "config-path", s.ConfigPath, "Path to the configuration file")
	fs.StringVar(&s.ConsoleAddr, "console-addr", s.ConsoleAddr, "If set, use as the address of the telnet-based console")
	fs.StringVar(&s.CloudProvider, "cloud-provider", s.CloudProvider, "If set, use the cloud provider to retrieve metadata about the sender")
	fs.StringVar(&s.CPUProfile, "cpu-profile", s.CPUProfile, "Use profiler and write results to this file")
	fs.StringSliceVar(&s.DefaultTags, "default-tags", s.DefaultTags, "Default tags to add to the metrics")
	fs.DurationVar(&s.ExpiryInterval, "expiry-interval", s.ExpiryInterval, "After how long do we expire metrics (0 to disable)")
	fs.DurationVar(&s.FlushInterval, "flush-interval", s.FlushInterval, "How often to flush metrics to the backends")
	fs.IntVar(&s.MaxReaders, "max-readers", s.MaxReaders, "Maximum number of socket readers")
	fs.IntVar(&s.MaxMessengers, "max-messengers", s.MaxMessengers, "Maximum number of workers to process messages")
	fs.IntVar(&s.MaxWorkers, "max-workers", s.MaxWorkers, "Maximum number of workers to process metrics")
	fs.StringVar(&s.MetricsAddr, "metrics-addr", s.MetricsAddr, "Address on which to listen for metrics")
	fs.StringVar(&s.Namespace, "namespace", s.Namespace, "Namespace all metrics")
	fs.StringSliceVar(&s.PercentThreshold, "percent-threshold", s.PercentThreshold, "Comma-separated list of percentiles")
	fs.BoolVar(&s.Verbose, "verbose", false, "Verbose")
	fs.BoolVar(&s.Version, "version", false, "Print the version and exit")
	fs.StringVar(&s.WebConsoleAddr, "web-addr", s.WebConsoleAddr, "If set, use as the address of the web-based console")
}

// Run runs the specified StatsdServer.
func (s *Server) Run() error {
	if s.Verbose {
		log.SetLevel(log.DebugLevel)
	}

	if s.ConfigPath != "" {
		viper.SetConfigFile(s.ConfigPath)
		err := viper.ReadInConfig()
		if err != nil {
			return err
		}
	}

	// Start the metric aggregator
	var backends []backend.MetricSender
	for _, backendName := range s.Backends {
		b, err := backend.InitBackend(backendName)
		if err != nil {
			return err
		}
		backends = append(backends, b)
	}

	var percentThresholds []float64
	for _, sPercentThreshold := range s.PercentThreshold {
		pt, err := strconv.ParseFloat(sPercentThreshold, 64)
		if err != nil {
			return err
		}
		percentThresholds = append(percentThresholds, pt)
	}

	aggregator := NewMetricAggregator(backends, percentThresholds, s.FlushInterval, s.ExpiryInterval, s.MaxWorkers, s.DefaultTags)
	go aggregator.Aggregate()
	s.aggregator = aggregator

	// Start the metric receiver
	f := func(metric types.Metric) {
		aggregator.MetricQueue <- metric
	}
	cloud, err := cloudprovider.InitCloudProvider(s.CloudProvider)
	if err != nil {
		return err
	}
	receiver := NewMetricReceiver(s.MetricsAddr, s.Namespace, s.MaxReaders, s.MaxMessengers, s.DefaultTags, cloud, HandlerFunc(f))
	go receiver.ListenAndReceive()

	// Start the console(s)
	if s.ConsoleAddr != "" {
		console := ConsoleServer{s.ConsoleAddr, aggregator}
		go console.ListenAndServe()
	}
	if s.WebConsoleAddr != "" {
		console := WebConsoleServer{s.WebConsoleAddr, aggregator}
		go console.ListenAndServe()
	}

	// Listen forever
	select {}
}

func internalStatName(name string) string {
	return fmt.Sprintf("statsd.%s", name)
}
