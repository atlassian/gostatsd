package graphite

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/backends/sender"
	"github.com/atlassian/gostatsd/pkg/transport"
	"github.com/atlassian/gostatsd/pkg/util"
)

const (
	// BackendName is the name of this backend.
	BackendName = "graphite"
	// DefaultAddress is the default address of Graphite server.
	DefaultAddress = "localhost:2003"
	// DefaultDialTimeout is the default net.Dial timeout.
	DefaultDialTimeout = 5 * time.Second
	// DefaultWriteTimeout is the default socket write timeout.
	DefaultWriteTimeout = 30 * time.Second
	// DefaultGlobalPrefix is the default global prefix.
	DefaultGlobalPrefix = "stats"
	// DefaultPrefixCounter is the default counters prefix.
	DefaultPrefixCounter = "counters"
	// DefaultPrefixTimer is the default timers prefix.
	DefaultPrefixTimer = "timers"
	// DefaultPrefixGauge is the default gauges prefix.
	DefaultPrefixGauge = "gauges"
	// DefaultPrefixSet is the default sets prefix.
	DefaultPrefixSet = "sets"
	// DefaultGlobalSuffix is the default global suffix.
	DefaultGlobalSuffix = ""
	// DefaultMode controls whether to use legacy namespace, no tags, or tags
	DefaultMode = "tags"
)

const (
	bufSize = 1 * 1024 * 1024
	// maxConcurrentSends is the number of max concurrent SendMetricsAsync calls that can actually make progress.
	// More calls will block. The current implementation uses maximum 1 call.
	maxConcurrentSends = 10
)

var (
	regWhitespace  = regexp.MustCompile(`\s+`)
	regNonAlphaNum = regexp.MustCompile(`[^a-zA-Z\d_.-]`)
)

// Client is an object that is used to send messages to a Graphite server's TCP interface.
type Client struct {
	sender           sender.Sender
	counterNamespace string // all strings have . stripped off start and end, and are normalized.
	timerNamespace   string
	gaugesNamespace  string
	setsNamespace    string
	globalSuffix     string
	legacyNamespace  bool
	enableTags       bool
	disabledSubtypes gostatsd.TimerSubtypes
}

func (client *Client) Run(ctx context.Context) {
	client.sender.Run(ctx)
}

// SendMetricsAsync flushes the metrics to the Graphite server, preparing payload synchronously but doing the send asynchronously.
func (client *Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	buf := client.preparePayload(metrics, time.Now())
	sink := make(chan *bytes.Buffer, 1)
	sink <- buf
	close(sink)
	select {
	case <-ctx.Done():
		client.sender.PutBuffer(buf)
		cb([]error{ctx.Err()})
	case client.sender.Sink <- sender.Stream{Ctx: ctx, Cb: cb, Buf: sink}:
	}
}

// normalizeMetricName will:
// - Replace:
// -- whitespace with "_"
// -- "/" with "-"
// - Delete:
// -- any character that is non alphanumeric, "_", ".", or "-"
func normalizeMetricName(s string) string {
	r1 := regWhitespace.ReplaceAllLiteral([]byte(s), []byte{'_'})
	r2 := bytes.Replace(r1, []byte{'/'}, []byte{'-'}, -1)
	return string(regNonAlphaNum.ReplaceAllLiteral(r2, nil))
}

// asGraphiteTag will convert a `key:value` or `value` tag to `key=value` or `unnamed=value`
func asGraphiteTag(tag string) string {
	if strings.Contains(tag, ":") {
		return strings.Replace(tag, ":", "=", 1)
	}
	return "unnamed=" + tag
}

// prepareName will create a metric name, handling correct prefix, suffixes, and tags, with an optional host tag if
// not overridden by a tag on the metric.
func (client *Client) prepareName(namespace, name, suffix, hostname string, tags gostatsd.Tags) string {
	buf := bytes.Buffer{}
	if namespace != "" {
		buf.WriteString(namespace)
		buf.WriteByte('.')
	}
	buf.WriteString(normalizeMetricName(name))
	if suffix != "" {
		buf.WriteByte('.')
		buf.WriteString(suffix)
	}
	if client.globalSuffix != "" {
		buf.WriteByte('.')
		buf.WriteString(client.globalSuffix)
	}

	if client.enableTags {
		haveHost := false
		for _, tag := range tags {
			graphiteTag := asGraphiteTag(tag)
			buf.WriteByte(';')
			buf.WriteString(graphiteTag)
			if strings.HasPrefix(tag, "host:") {
				haveHost = true
			}
		}
		if !haveHost && hostname != "" {
			buf.WriteString(";host=")
			buf.WriteString(hostname)
		}
	}

	return buf.String()
}

func (client *Client) preparePayload(metrics *gostatsd.MetricMap, ts time.Time) *bytes.Buffer {
	buf := client.sender.GetBuffer()
	now := ts.Unix()
	if client.legacyNamespace {
		metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
			_, _ = fmt.Fprintf(buf, "%s %d %d\n", client.prepareName("stats_counts", key, "", counter.Hostname, counter.Tags), counter.Value, now)
			_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.counterNamespace, key, "", counter.Hostname, counter.Tags), counter.PerSecond, now)
		})
	} else {
		metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
			_, _ = fmt.Fprintf(buf, "%s %d %d\n", client.prepareName(client.counterNamespace, key, "count", counter.Hostname, counter.Tags), counter.Value, now)
			_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.counterNamespace, key, "rate", counter.Hostname, counter.Tags), counter.PerSecond, now)
		})
	}
	metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		if timer.Histogram != nil {
			for histogramThreshold, count := range timer.Histogram {
				bucketTag := "le:+Inf"
				if !math.IsInf(float64(histogramThreshold), 1) {
					bucketTag = "le:" + strconv.FormatFloat(float64(histogramThreshold), 'f', -1, 64)
				}
				newTags := timer.Tags.Concat(gostatsd.Tags{bucketTag})
				_, _ = fmt.Fprintf(buf, "%s %d %d\n", client.prepareName(client.counterNamespace, key, "histogram", timer.Hostname, newTags), count, now)
			}
		} else {
			if !client.disabledSubtypes.Lower {
				_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "lower", timer.Hostname, timer.Tags), timer.Min, now)
			}
			if !client.disabledSubtypes.Upper {
				_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "upper", timer.Hostname, timer.Tags), timer.Max, now)
			}
			if !client.disabledSubtypes.Count {
				_, _ = fmt.Fprintf(buf, "%s %d %d\n", client.prepareName(client.timerNamespace, key, "count", timer.Hostname, timer.Tags), timer.Count, now)
			}
			if !client.disabledSubtypes.CountPerSecond {
				_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "count_ps", timer.Hostname, timer.Tags), timer.PerSecond, now)
			}
			if !client.disabledSubtypes.Mean {
				_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "mean", timer.Hostname, timer.Tags), timer.Mean, now)
			}
			if !client.disabledSubtypes.Median {
				_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "median", timer.Hostname, timer.Tags), timer.Median, now)
			}
			if !client.disabledSubtypes.StdDev {
				_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "std", timer.Hostname, timer.Tags), timer.StdDev, now)
			}
			if !client.disabledSubtypes.Sum {
				_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "sum", timer.Hostname, timer.Tags), timer.Sum, now)
			}
			if !client.disabledSubtypes.SumSquares {
				_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "sum_squares", timer.Hostname, timer.Tags), timer.SumSquares, now)
			}
			for _, pct := range timer.Percentiles {
				_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, pct.Str, timer.Hostname, timer.Tags), pct.Float, now)
			}
		}
	})
	metrics.Gauges.Each(func(key, tagsKey string, gauge gostatsd.Gauge) {
		_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.gaugesNamespace, key, "", gauge.Hostname, gauge.Tags), gauge.Value, now)
	})
	metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		_, _ = fmt.Fprintf(buf, "%s %d %d\n", client.prepareName(client.setsNamespace, key, "", set.Hostname, set.Tags), len(set.Values), now)
	})
	return buf
}

// SendEvent discards events.
func (client *Client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	return nil
}

// Name returns the name of the backend.
func (client *Client) Name() string {
	return BackendName
}

// NewClientFromViper constructs a Client object using configuration provided by Viper
func NewClientFromViper(v *viper.Viper, logger logrus.FieldLogger, pool *transport.TransportPool) (gostatsd.Backend, error) {
	g := util.GetSubViper(v, "graphite")
	g.SetDefault("address", DefaultAddress)
	g.SetDefault("dial_timeout", DefaultDialTimeout)
	g.SetDefault("write_timeout", DefaultWriteTimeout)
	g.SetDefault("global_prefix", DefaultGlobalPrefix)
	g.SetDefault("prefix_counter", DefaultPrefixCounter)
	g.SetDefault("prefix_timer", DefaultPrefixTimer)
	g.SetDefault("prefix_gauge", DefaultPrefixGauge)
	g.SetDefault("prefix_set", DefaultPrefixSet)
	g.SetDefault("global_suffix", DefaultGlobalSuffix)
	g.SetDefault("mode", DefaultMode)
	return NewClient(
		g.GetString("address"),
		g.GetDuration("dial_timeout"),
		g.GetDuration("write_timeout"),
		g.GetString("global_prefix"),
		g.GetString("prefix_counter"),
		g.GetString("prefix_timer"),
		g.GetString("prefix_gauge"),
		g.GetString("prefix_set"),
		g.GetString("global_suffix"),
		g.GetString("mode"),
		gostatsd.DisabledSubMetrics(v),
		logger,
	)
}

// NewClient constructs a Graphite backend object.
func NewClient(
	address string,
	dialTimeout time.Duration,
	writeTimeout time.Duration,
	globalPrefix string,
	prefixCounter string,
	prefixTimer string,
	prefixGauge string,
	prefixSet string,
	globalSuffix string,
	mode string,
	disabled gostatsd.TimerSubtypes,
	logger logrus.FieldLogger,
) (*Client, error) {
	if address == "" {
		return nil, fmt.Errorf("[%s] address is required", BackendName)
	}
	if dialTimeout <= 0 {
		return nil, fmt.Errorf("[%s] dialTimeout should be positive", BackendName)
	}
	if writeTimeout < 0 {
		return nil, fmt.Errorf("[%s] writeTimeout should be non-negative", BackendName)
	}
	globalSuffix = strings.Trim(globalSuffix, ".")

	var legacyNamespace, enableTags bool
	switch mode {
	case "legacy":
		legacyNamespace = true
		enableTags = false
	case "basic":
		legacyNamespace = false
		enableTags = false
	case "tags":
		legacyNamespace = false
		enableTags = true
	default:
		return nil, fmt.Errorf("[%s] mode must be one of 'legacy', 'basic', or 'tags'", BackendName)
	}

	var counterNamespace, timerNamespace, gaugesNamespace, setsNamespace string

	if legacyNamespace {
		counterNamespace = DefaultGlobalPrefix
		timerNamespace = combine(DefaultGlobalPrefix, "timers")
		gaugesNamespace = combine(DefaultGlobalPrefix, "gauges")
		setsNamespace = combine(DefaultGlobalPrefix, "sets")
	} else {
		globalPrefix := globalPrefix
		counterNamespace = combine(globalPrefix, prefixCounter)
		timerNamespace = combine(globalPrefix, prefixTimer)
		gaugesNamespace = combine(globalPrefix, prefixGauge)
		setsNamespace = combine(globalPrefix, prefixSet)
	}

	counterNamespace = normalizeMetricName(counterNamespace)
	timerNamespace = normalizeMetricName(timerNamespace)
	gaugesNamespace = normalizeMetricName(gaugesNamespace)
	setsNamespace = normalizeMetricName(setsNamespace)
	globalSuffix = normalizeMetricName(globalSuffix)

	logger.WithFields(logrus.Fields{
		"address":           address,
		"dial-timeout":      dialTimeout,
		"write-timeout":     writeTimeout,
		"counter-namespace": counterNamespace,
		"timer-namespace":   timerNamespace,
		"gauges-namespace":  gaugesNamespace,
		"sets-namespace":    setsNamespace,
		"global-suffix":     globalSuffix,
		"mode":              mode,
	}).Info("created backend")

	return &Client{
		sender: sender.Sender{
			Logger: logger,
			ConnFactory: func() (net.Conn, error) {
				return net.DialTimeout("tcp", address, dialTimeout)
			},
			Sink: make(chan sender.Stream, maxConcurrentSends),
			BufPool: sync.Pool{
				New: func() interface{} {
					buf := new(bytes.Buffer)
					buf.Grow(bufSize)
					return buf
				},
			},
			WriteTimeout: writeTimeout,
		},
		counterNamespace: counterNamespace,
		timerNamespace:   timerNamespace,
		gaugesNamespace:  gaugesNamespace,
		setsNamespace:    setsNamespace,
		globalSuffix:     globalSuffix,
		legacyNamespace:  legacyNamespace,
		enableTags:       enableTags,
		disabledSubtypes: disabled,
	}, nil
}

func combine(prefix, suffix string) string {
	prefix = strings.Trim(prefix, ".")
	suffix = strings.Trim(suffix, ".")
	if prefix != "" && suffix != "" {
		return prefix + "." + suffix
	}
	if prefix != "" {
		return prefix
	}
	return suffix
}
