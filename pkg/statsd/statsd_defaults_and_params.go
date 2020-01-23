package statsd

import (
	"github.com/atlassian/gostatsd/pkg/cloudproviders/aws"
	"github.com/atlassian/gostatsd/pkg/cloudproviders/k8s"
	"runtime"
	"time"

	"strings"

	"github.com/atlassian/gostatsd"

	"github.com/spf13/pflag"
)

// DefaultBackends is the list of default backends' names.
var DefaultBackends = []string{"graphite"}

// DefaultMaxReaders is the default number of socket reading goroutines.
var DefaultMaxReaders = minInt(8, runtime.NumCPU())

// DefaultMaxWorkers is the default number of goroutines that aggregate metrics.
var DefaultMaxWorkers = runtime.NumCPU()

// DefaultMaxParsers is the default number of goroutines that parse datagrams into metrics.
var DefaultMaxParsers = runtime.NumCPU()

// DefaultPercentThreshold is the default list of applied percentiles.
var DefaultPercentThreshold = []float64{90}

// DefaultTags is the default list of additional tags.
var DefaultTags = gostatsd.Tags{}

// DefaultInternalTags is the default list of additional tags on internal metrics
var DefaultInternalTags = gostatsd.Tags{}

// DefaultCloudProviderCacheValues contains the default cache values for each cloud provider.
var DefaultCloudProviderCacheValues = map[string]CacheOptions{
	k8s.ProviderName: {
		// Basically the CacheRefreshPeriod is the granularity at which we care about batching data.
		// If 5 requests come in inside 50 milliseconds, and this is set to 50 milliseconds, we only need to look
		// up the informer once, and do the regexes once. This saves us resources at the cost of data precision.
		CacheRefreshPeriod:        50 * time.Millisecond,
		CacheEvictAfterIdlePeriod: 1 * time.Millisecond,
		CacheTTL:                  1 * time.Millisecond,
		CacheNegativeTTL:          1 * time.Millisecond,
	},
	aws.ProviderName: {
		CacheRefreshPeriod:        DefaultCacheRefreshPeriod,
		CacheEvictAfterIdlePeriod: DefaultCacheEvictAfterIdlePeriod,
		CacheTTL:                  DefaultCacheTTL,
		CacheNegativeTTL:          DefaultCacheNegativeTTL,
	},
}

type LimiterValues struct {
	MaxCloudRequests   int
	BurstCloudRequests int
}

// DefaultCloudProviderLimiterValues contains the default limiter values for each cloud provider.
var DefaultCloudProviderLimiterValues = map[string]LimiterValues{
	k8s.ProviderName: {
		// High limit for k8s since we don't make network requests for each data request
		MaxCloudRequests:   10000,
		BurstCloudRequests: 5000,
	},
	aws.ProviderName: {
		MaxCloudRequests:   DefaultMaxCloudRequests,
		BurstCloudRequests: DefaultBurstCloudRequests,
	},
}

const (
	// StatserInternal is the name used to indicate the use of the internal statser.
	StatserInternal = "internal"
	// StatserLogging is the name used to indicate the use of the logging statser.
	StatserLogging = "logging"
	// StatserNull is the name used to indicate the use of the null statser.
	StatserNull = "null"
	// StatserTagged is the name used to indicate the use of the tagged statser.
	StatserTagged = "tagged"
)

const (
	// DefaultMaxCloudRequests is the maximum number of cloud provider requests per second.
	DefaultMaxCloudRequests = 10
	// DefaultBurstCloudRequests is the burst number of cloud provider requests per second.
	DefaultBurstCloudRequests = DefaultMaxCloudRequests + 5
	// DefaultExpiryInterval is the default expiry interval for metrics.
	DefaultExpiryInterval = 5 * time.Minute
	// DefaultFlushInterval is the default metrics flush interval.
	DefaultFlushInterval = 1 * time.Second
	// DefaultIgnoreHost is the default value for whether the source should be used as the host
	DefaultIgnoreHost = false
	// DefaultMetricsAddr is the default address on which to listen for metrics.
	DefaultMetricsAddr = ":8125"
	// DefaultMaxQueueSize is the default maximum number of buffered metrics per worker.
	DefaultMaxQueueSize = 10000 // arbitrary
	// DefaultMaxConcurrentEvents is the default maximum number of events sent concurrently.
	DefaultMaxConcurrentEvents = 1024 // arbitrary
	// DefaultCacheRefreshPeriod is the default cache refresh period.
	DefaultCacheRefreshPeriod = 1 * time.Minute
	// DefaultCacheEvictAfterIdlePeriod is the default idle cache eviction period.
	DefaultCacheEvictAfterIdlePeriod = 10 * time.Minute
	// DefaultCacheTTL is the default cache TTL for successful lookups.
	DefaultCacheTTL = 30 * time.Minute
	// DefaultCacheNegativeTTL is the default cache TTL for failed lookups (errors or when instance was not found).
	DefaultCacheNegativeTTL = 1 * time.Minute
	// DefaultInternalNamespace is the default internal namespace
	DefaultInternalNamespace = "statsd"
	// DefaultHeartbeatEnabled is the default heartbeat enabled flag
	DefaultHeartbeatEnabled = false
	// DefaultReceiveBatchSize is the number of datagrams to read in each receive batch
	DefaultReceiveBatchSize = 50
	// DefaultEstimatedTags is the estimated number of expected tags on an individual metric submitted externally
	DefaultEstimatedTags = 4
	// DefaultConnPerReader is the default for whether to create a connection per reader
	DefaultConnPerReader = false
	// DefaultStatserType is the default statser type
	DefaultStatserType = StatserInternal
	// DefaultBadLinesPerMinute is the default number of bad lines to allow to log per minute
	DefaultBadLinesPerMinute = 0
	// DefaultServerMode is the default mode to run as, standalone|forwarder
	DefaultServerMode = "standalone"
	// DefaultLogRawMetric is the default value for whether to log the metrics received from network
	DefaultLogRawMetric = false
)

const (
	// ParamBackends is the name of parameter with backends.
	ParamBackends = "backends"
	// ParamCloudProvider is the name of parameter with the name of cloud provider.
	ParamCloudProvider = "cloud-provider"
	// ParamMaxCloudRequests is the name of parameter with maximum number of cloud provider requests per second.
	ParamMaxCloudRequests = "max-cloud-requests"
	// ParamBurstCloudRequests is the name of parameter with burst number of cloud provider requests per second.
	ParamBurstCloudRequests = "burst-cloud-requests"
	// ParamDefaultTags is the name of parameter with the list of additional tags.
	ParamDefaultTags = "default-tags"
	// ParamInternalTags is the name of parameter with the list of tags for internal metrics.
	ParamInternalTags = "internal-tags"
	// ParamInternalNamespace is the name of parameter with the namespace for internal metrics.
	ParamInternalNamespace = "internal-namespace"
	// ParamExpiryInterval is the name of parameter with expiry interval for metrics.
	ParamExpiryInterval = "expiry-interval"
	// ParamFlushInterval is the name of parameter with metrics flush interval.
	ParamFlushInterval = "flush-interval"
	// ParamIgnoreHost is the name of parameter indicating if the source should be used as the host
	ParamIgnoreHost = "ignore-host"
	// ParamMaxReaders is the name of parameter with number of socket readers.
	ParamMaxReaders = "max-readers"
	// ParamMaxParsers is the name of the parameter with the number of goroutines that parse datagrams into metrics.
	ParamMaxParsers = "max-parsers"
	// ParamMaxWorkers is the name of parameter with number of goroutines that aggregate metrics.
	ParamMaxWorkers = "max-workers"
	// ParamMaxQueueSize is the name of parameter with maximum number of buffered metrics per worker.
	ParamMaxQueueSize = "max-queue-size"
	// ParamMaxConcurrentEvents is the name of parameter with maximum number of events sent concurrently.
	ParamMaxConcurrentEvents = "max-concurrent-events"
	// ParamEstimatedTags is the name of parameter with estimated number of tags per metric
	ParamEstimatedTags = "estimated-tags"
	// ParamCacheRefreshPeriod is the name of parameter with cache refresh period.
	ParamCacheRefreshPeriod = "cloud-cache-refresh-period"
	// ParamCacheEvictAfterIdlePeriod is the name of parameter with idle cache eviction period.
	ParamCacheEvictAfterIdlePeriod = "cloud-cache-evict-after-idle-period"
	// ParamCacheTTL is the name of parameter with cache TTL for successful lookups.
	ParamCacheTTL = "cloud-cache-ttl"
	// ParamCacheNegativeTTL is the name of parameter with cache TTL for failed lookups (errors or when instance was not found).
	ParamCacheNegativeTTL = "cloud-cache-negative-ttl"
	// ParamMetricsAddr is the name of parameter with address on which to listen for metrics.
	ParamMetricsAddr = "metrics-addr"
	// ParamNamespace is the name of parameter with namespace for all metrics.
	ParamNamespace = "namespace"
	// ParamStatserType is the name of parameter with type of statser.
	ParamStatserType = "statser-type"
	// ParamPercentThreshold is the name of parameter with list of applied percentiles.
	ParamPercentThreshold = "percent-threshold"
	// ParamHeartbeatEnabled is the name of the parameter with the heartbeat enabled
	ParamHeartbeatEnabled = "heartbeat-enabled"
	// ParamReceiveBatchSize is the name of the parameter with the number of datagrams to read in each receive batch
	ParamReceiveBatchSize = "receive-batch-size"
	// ParamConnPerReader is the name of the parameter indicating whether to create a connection per reader
	ParamConnPerReader = "conn-per-reader"
	// ParamBadLineRateLimitPerMinute is the name of the parameter indicating how many bad lines can be logged per minute
	ParamBadLinesPerMinute = "bad-lines-per-minute"
	// ParamServerMode is the name of the parameter used to configure the server mode.
	ParamServerMode = "server-mode"
	// ParamHostname allows hostname overrides
	ParamHostname = "hostname"
	// ParamLogRawMetric enables custom metrics to be printed to stdout
	ParamLogRawMetric = "log-raw-metric"
)

// AddFlags adds flags to the specified FlagSet.
func AddFlags(fs *pflag.FlagSet) {
	fs.String(ParamCloudProvider, "", "If set, use the cloud provider to retrieve metadata about the sender")
	fs.Duration(ParamExpiryInterval, DefaultExpiryInterval, "After how long do we expire metrics (0 to disable)")
	fs.Duration(ParamFlushInterval, DefaultFlushInterval, "How often to flush metrics to the backends")
	fs.Bool(ParamIgnoreHost, DefaultIgnoreHost, "Ignore the source for populating the hostname field of metrics")
	fs.Int(ParamMaxReaders, DefaultMaxReaders, "Maximum number of socket readers")
	fs.Int(ParamMaxParsers, DefaultMaxParsers, "Maximum number of workers to parse datagrams into metrics")
	fs.Int(ParamMaxWorkers, DefaultMaxWorkers, "Maximum number of workers to process metrics")
	fs.Int(ParamMaxQueueSize, DefaultMaxQueueSize, "Maximum number of buffered metrics per worker")
	fs.Int(ParamMaxConcurrentEvents, DefaultMaxConcurrentEvents, "Maximum number of events sent concurrently")
	fs.Int(ParamEstimatedTags, DefaultEstimatedTags, "Estimated number of expected tags on an individual metric submitted externally")
	fs.Duration(ParamCacheRefreshPeriod, DefaultCacheRefreshPeriod, "Cloud cache refresh period")
	fs.Duration(ParamCacheEvictAfterIdlePeriod, DefaultCacheEvictAfterIdlePeriod, "Idle cloud cache eviction period")
	fs.Duration(ParamCacheTTL, DefaultCacheTTL, "Cloud cache TTL for successful lookups")
	fs.Duration(ParamCacheNegativeTTL, DefaultCacheNegativeTTL, "Cloud cache TTL for failed lookups")
	fs.String(ParamMetricsAddr, DefaultMetricsAddr, "Address on which to listen for metrics")
	fs.String(ParamNamespace, "", "Namespace all metrics")
	fs.String(ParamBackends, strings.Join(DefaultBackends, " "), "Space separated list of backends")
	fs.Int(ParamMaxCloudRequests, DefaultMaxCloudRequests, "Maximum number of cloud provider requests per second")
	fs.Int(ParamBurstCloudRequests, DefaultBurstCloudRequests, "Burst number of cloud provider requests per second")
	fs.String(ParamDefaultTags, strings.Join(DefaultTags, " "), "Space separated list of tags to add to all metrics")
	fs.String(ParamInternalTags, strings.Join(DefaultInternalTags, " "), "Space separated list of tags to add to internal metrics")
	fs.String(ParamInternalNamespace, DefaultInternalNamespace, "Namespace for internal metrics, may be \"\"")
	fs.String(ParamStatserType, DefaultStatserType, "Statser type to be used for sending metrics")
	fs.String(ParamPercentThreshold, strings.Join(toStringSlice(DefaultPercentThreshold), " "), "Space separated list of percentiles")
	fs.Bool(ParamHeartbeatEnabled, DefaultHeartbeatEnabled, "Enables heartbeat")
	fs.Int(ParamReceiveBatchSize, DefaultReceiveBatchSize, "The number of datagrams to read in each receive batch")
	fs.Bool(ParamConnPerReader, DefaultConnPerReader, "Create a separate connection per reader (requires system support for reusing addresses)")
	fs.String(ParamServerMode, DefaultServerMode, "The server mode to run in")
	fs.String(ParamHostname, getHost(), "overrides the hostname of the server")
	fs.Bool(ParamLogRawMetric, DefaultLogRawMetric, "Print metrics received from network to stdout in JSON format")
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
