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

	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

var DefaultBackends = []string{"graphite"}
var DefaultMaxReaders = runtime.NumCPU()
var DefaultMaxMessengers = runtime.NumCPU() * 8
var DefaultMaxWorkers = runtime.NumCPU() * 8 * 8
var DefaultPercentThreshold = []string{"90"}
var DefaultTags = []string{""}

const (
	DefaultExpiryInterval = 5 * time.Minute
	DefaultFlushInterval  = 1 * time.Second
	DefaultMetricsAddr    = ":8125"
)

const (
	ParamBackends         = "backends"
	ParamConsoleAddr      = "console-addr"
	ParamCloudProvider    = "cloud-provider"
	ParamDefaultTags      = "default-tags"
	ParamExpiryInterval   = "expiry-interval"
	ParamFlushInterval    = "flush-interval"
	ParamMaxReaders       = "max-readers"
	ParamMaxMessengers    = "max-messengers"
	ParamMaxWorkers       = "max-workers"
	ParamMetricsAddr      = "metrics-addr"
	ParamNamespace        = "namespace"
	ParamPercentThreshold = "percent-threshold"
	ParamWebAddr          = "web-addr"
)

// Server encapsulates all of the parameters necessary for starting up
// the statsd server. These can either be set via command line or directly.
type Server struct {
	aggregator       *MetricAggregator
	Backends         []string
	ConsoleAddr      string
	CloudProvider    string
	DefaultTags      []string
	ExpiryInterval   time.Duration
	FlushInterval    time.Duration
	MaxReaders       int
	MaxWorkers       int
	MaxMessengers    int
	MetricsAddr      string
	Namespace        string
	PercentThreshold []string
	WebConsoleAddr   string
	Viper            *viper.Viper
}

// NewServer will create a new Server with the default configuration.
func NewServer() *Server {
	return &Server{
		Backends:         DefaultBackends,
		ConsoleAddr:      DefaultConsoleAddr,
		DefaultTags:      DefaultTags,
		ExpiryInterval:   DefaultExpiryInterval,
		FlushInterval:    DefaultFlushInterval,
		MaxReaders:       DefaultMaxReaders,
		MaxMessengers:    DefaultMaxMessengers,
		MaxWorkers:       DefaultMaxWorkers,
		MetricsAddr:      DefaultMetricsAddr,
		PercentThreshold: DefaultPercentThreshold,
		WebConsoleAddr:   DefaultWebConsoleAddr,
		Viper:            viper.New(),
	}
}

// AddFlags adds flags to the specified FlagSet
func AddFlags(fs *pflag.FlagSet) {
	fs.String(ParamConsoleAddr, DefaultConsoleAddr, "If set, use as the address of the telnet-based console")
	fs.String(ParamCloudProvider, "", "If set, use the cloud provider to retrieve metadata about the sender")
	fs.Duration(ParamExpiryInterval, DefaultExpiryInterval, "After how long do we expire metrics (0 to disable)")
	fs.Duration(ParamFlushInterval, DefaultFlushInterval, "How often to flush metrics to the backends")
	fs.Int(ParamMaxReaders, DefaultMaxReaders, "Maximum number of socket readers")
	fs.Int(ParamMaxMessengers, DefaultMaxMessengers, "Maximum number of workers to process messages")
	fs.Int(ParamMaxWorkers, DefaultMaxWorkers, "Maximum number of workers to process metrics")
	fs.String(ParamMetricsAddr, DefaultMetricsAddr, "Address on which to listen for metrics")
	fs.String(ParamNamespace, "", "Namespace all metrics")
	fs.String(ParamWebAddr, DefaultWebConsoleAddr, "If set, use as the address of the web-based console")
	//TODO Remove workaround when https://github.com/spf13/viper/issues/112 is fixed
	fs.String(ParamBackends, strings.Join(DefaultBackends, ","), "Comma-separated list of backends")
	fs.String(ParamDefaultTags, strings.Join(DefaultTags, ","), "Comma-separated list of tags to add to all metrics")
	fs.String(ParamPercentThreshold, strings.Join(DefaultPercentThreshold, ","), "Comma-separated list of percentiles")
}

// Run runs the server until context signals done.
func (s *Server) Run(ctx context.Context) error {
	// Start the metric aggregator
	backends := make([]backend.MetricSender, 0, len(s.Backends))
	for _, backendName := range s.Backends {
		b, err := backend.InitBackend(backendName, s.Viper)
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
	f := func(metric *types.Metric) {
		aggregator.MetricQueue <- metric
	}
	cloud, err := cloudprovider.InitCloudProvider(s.CloudProvider, s.Viper)
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

	// Listen until done
	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}

func internalStatName(name string) string {
	return fmt.Sprintf("statsd.%s", name)
}
