package graphite

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"regexp"
	"time"

	backendTypes "github.com/atlassian/gostatsd/backend/types"
	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
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
	// DefaultLegacyNamespace controls whether legacy namespace should be used by default.
	DefaultLegacyNamespace = true
)

const sampleConfig = `
[graphite]
	# graphite host or ip address
	address = "ip:2003"
`

var (
	regWhitespace  = regexp.MustCompile(`\s+`)
	regNonAlphaNum = regexp.MustCompile(`[^a-zA-Z\d_.-]`)
)

// Config holds configuration for the Graphite backend.
type Config struct {
	Address         *string
	DialTimeout     *time.Duration
	WriteTimeout    *time.Duration
	GlobalPrefix    *string
	PrefixCounter   *string
	PrefixTimer     *string
	PrefixGauge     *string
	PrefixSet       *string
	GlobalSuffix    *string
	LegacyNamespace *bool
}

// client is an object that is used to send messages to a Graphite server's TCP interface.
type client struct {
	address          string
	dialTimeout      time.Duration
	writeTimeout     time.Duration
	counterNamespace string
	timerNamespace   string
	gaugesNamespace  string
	setsNamespace    string
	globalSuffix     string
	legacyNamespace  bool
}

// SendMetricsAsync flushes the metrics to the Graphite server, preparing payload synchronously but doing the send asynchronously.
func (client *client) SendMetricsAsync(ctx context.Context, metrics *types.MetricMap, cb backendTypes.SendCallback) {
	if metrics.NumStats == 0 {
		cb(nil)
		return
	}
	buf := client.preparePayload(metrics, time.Now())
	go func() {
		cb([]error{client.doSend(ctx, buf)})
	}()
}

func (client *client) doSend(ctx context.Context, buf *bytes.Buffer) (retErr error) {
	conn, err := net.DialTimeout("tcp", client.address, client.dialTimeout)
	if err != nil {
		return fmt.Errorf("[%s] error connecting: %v", BackendName, err)
	}
	defer func() {
		errClose := conn.Close()
		if errClose != nil && retErr == nil {
			retErr = fmt.Errorf("[%s] error sending: %v", BackendName, errClose)
		}
	}()
	if client.writeTimeout > 0 {
		if err = conn.SetWriteDeadline(time.Now().Add(client.writeTimeout)); err != nil {
			log.Warnf("[%s] failed to set write deadline: %v", BackendName, err)
		}
	}
	_, err = conn.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("[%s] error sending: %v", BackendName, err)
	}
	return nil
}

func (client *client) preparePayload(metrics *types.MetricMap, ts time.Time) *bytes.Buffer {
	buf := new(bytes.Buffer)
	now := ts.Unix()
	if client.legacyNamespace {
		metrics.Counters.Each(func(key, tagsKey string, counter types.Counter) {
			k := sk(key)
			fmt.Fprintf(buf, "stats_counts.%s%s %d %d\n", k, client.globalSuffix, counter.Value, now)
			fmt.Fprintf(buf, "%s%s%s %f %d\n", client.counterNamespace, k, client.globalSuffix, counter.PerSecond, now)
		})
	} else {
		metrics.Counters.Each(func(key, tagsKey string, counter types.Counter) {
			k := sk(key)
			fmt.Fprintf(buf, "%s%s.count%s %d %d\n", client.counterNamespace, k, client.globalSuffix, counter.Value, now)
			fmt.Fprintf(buf, "%s%s.rate%s %f %d\n", client.counterNamespace, k, client.globalSuffix, counter.PerSecond, now)
		})
	}
	metrics.Timers.Each(func(key, tagsKey string, timer types.Timer) {
		k := sk(key)
		fmt.Fprintf(buf, "%s%s.lower%s %f %d\n", client.timerNamespace, k, client.globalSuffix, timer.Min, now)
		fmt.Fprintf(buf, "%s%s.upper%s %f %d\n", client.timerNamespace, k, client.globalSuffix, timer.Max, now)
		fmt.Fprintf(buf, "%s%s.count%s %d %d\n", client.timerNamespace, k, client.globalSuffix, timer.Count, now)
		fmt.Fprintf(buf, "%s%s.count_ps%s %f %d\n", client.timerNamespace, k, client.globalSuffix, timer.PerSecond, now)
		fmt.Fprintf(buf, "%s%s.mean%s %f %d\n", client.timerNamespace, k, client.globalSuffix, timer.Mean, now)
		fmt.Fprintf(buf, "%s%s.median%s %f %d\n", client.timerNamespace, k, client.globalSuffix, timer.Median, now)
		fmt.Fprintf(buf, "%s%s.std%s %f %d\n", client.timerNamespace, k, client.globalSuffix, timer.StdDev, now)
		fmt.Fprintf(buf, "%s%s.sum%s %f %d\n", client.timerNamespace, k, client.globalSuffix, timer.Sum, now)
		fmt.Fprintf(buf, "%s%s.sum_squares%s %f %d\n", client.timerNamespace, k, client.globalSuffix, timer.SumSquares, now)
		for _, pct := range timer.Percentiles {
			fmt.Fprintf(buf, "%s%s.%s%s %f %d\n", client.timerNamespace, k, pct.Str, client.globalSuffix, pct.Float, now)
		}
	})
	metrics.Gauges.Each(func(key, tagsKey string, gauge types.Gauge) {
		fmt.Fprintf(buf, "%s%s%s %f %d\n", client.gaugesNamespace, sk(key), client.globalSuffix, gauge.Value, now)
	})
	metrics.Sets.Each(func(key, tagsKey string, set types.Set) {
		fmt.Fprintf(buf, "%s%s%s %d %d\n", client.setsNamespace, sk(key), client.globalSuffix, len(set.Values), now)
	})
	return buf
}

// SendEvent discards events.
func (client *client) SendEvent(ctx context.Context, e *types.Event) error {
	return nil
}

// SampleConfig returns the sample config for the graphite backend.
func (client *client) SampleConfig() string {
	return sampleConfig
}

// BackendName returns the name of the backend.
func (client *client) BackendName() string {
	return BackendName
}

// NewClientFromViper constructs a GraphiteClient object by connecting to an address.
func NewClientFromViper(v *viper.Viper) (backendTypes.Backend, error) {
	g := getSubViper(v, "graphite")
	g.SetDefault("address", DefaultAddress)
	g.SetDefault("dial_timeout", DefaultDialTimeout)
	g.SetDefault("write_timeout", DefaultWriteTimeout)
	g.SetDefault("global_prefix", DefaultGlobalPrefix)
	g.SetDefault("prefix_counter", DefaultPrefixCounter)
	g.SetDefault("prefix_timer", DefaultPrefixTimer)
	g.SetDefault("prefix_gauge", DefaultPrefixGauge)
	g.SetDefault("prefix_set", DefaultPrefixSet)
	g.SetDefault("global_suffix", DefaultGlobalSuffix)
	g.SetDefault("legacy_namespace", DefaultLegacyNamespace)
	return NewClient(&Config{
		Address:         addr(g.GetString("address")),
		DialTimeout:     addrD(g.GetDuration("dial_timeout")),
		WriteTimeout:    addrD(g.GetDuration("write_timeout")),
		GlobalPrefix:    addr(g.GetString("global_prefix")),
		PrefixCounter:   addr(g.GetString("prefix_counter")),
		PrefixTimer:     addr(g.GetString("prefix_timer")),
		PrefixGauge:     addr(g.GetString("prefix_gauge")),
		PrefixSet:       addr(g.GetString("prefix_set")),
		GlobalSuffix:    addr(g.GetString("global_suffix")),
		LegacyNamespace: addrB(g.GetBool("legacy_namespace")),
	})
}

// NewClient constructs a Graphite backend object.
func NewClient(config *Config) (backendTypes.Backend, error) {
	address := getOrDefaultStr(config.Address, DefaultAddress)
	if address == "" {
		return nil, fmt.Errorf("[%s] address is required", BackendName)
	}
	dialTimeout := getOrDefaultDur(config.DialTimeout, DefaultDialTimeout)
	if dialTimeout <= 0 {
		return nil, fmt.Errorf("[%s] dialTimeout should be positive", BackendName)
	}
	writeTimeout := getOrDefaultDur(config.WriteTimeout, DefaultWriteTimeout)
	if writeTimeout < 0 {
		return nil, fmt.Errorf("[%s] writeTimeout should be non-negative", BackendName)
	}
	globalSuffix := getOrDefaultStr(config.GlobalSuffix, DefaultGlobalSuffix)
	if globalSuffix != "" {
		globalSuffix = `.` + globalSuffix
	}
	var counterNamespace, timerNamespace, gaugesNamespace, setsNamespace string
	var legacyNamespace bool
	if config.LegacyNamespace != nil {
		legacyNamespace = *config.LegacyNamespace
	} else {
		legacyNamespace = DefaultLegacyNamespace
	}
	if legacyNamespace {
		counterNamespace = DefaultGlobalPrefix + `.`
		timerNamespace = DefaultGlobalPrefix + ".timers."
		gaugesNamespace = DefaultGlobalPrefix + ".gauges."
		setsNamespace = DefaultGlobalPrefix + ".sets."
	} else {
		globalPrefix := getOrDefaultPrefix(config.GlobalPrefix, DefaultGlobalPrefix)
		counterNamespace = globalPrefix + getOrDefaultPrefix(config.PrefixCounter, DefaultPrefixCounter)
		timerNamespace = globalPrefix + getOrDefaultPrefix(config.PrefixTimer, DefaultPrefixTimer)
		gaugesNamespace = globalPrefix + getOrDefaultPrefix(config.PrefixGauge, DefaultPrefixGauge)
		setsNamespace = globalPrefix + getOrDefaultPrefix(config.PrefixSet, DefaultPrefixSet)
	}
	log.Infof("[%s] address=%s dialTimeout=%s writeTimeout=%s", BackendName, address, dialTimeout, writeTimeout)
	return &client{
		address:          address,
		dialTimeout:      dialTimeout,
		writeTimeout:     writeTimeout,
		counterNamespace: counterNamespace,
		timerNamespace:   timerNamespace,
		gaugesNamespace:  gaugesNamespace,
		setsNamespace:    setsNamespace,
		globalSuffix:     globalSuffix,
		legacyNamespace:  legacyNamespace,
	}, nil
}

func getOrDefaultPrefix(val *string, def string) string {
	v := getOrDefaultStr(val, def)
	if v != "" {
		return v + `.`
	}
	return ""
}

func getOrDefaultStr(val *string, def string) string {
	if val != nil {
		return *val
	}
	return def
}

func getOrDefaultDur(val *time.Duration, def time.Duration) time.Duration {
	if val != nil {
		return *val
	}
	return def
}

func sk(s string) []byte {
	r1 := regWhitespace.ReplaceAllLiteral([]byte(s), []byte{'_'})
	r2 := bytes.Replace(r1, []byte{'/'}, []byte{'-'}, -1)
	return regNonAlphaNum.ReplaceAllLiteral(r2, nil)
}

func addr(s string) *string {
	return &s
}

func addrB(b bool) *bool {
	return &b
}

func addrD(d time.Duration) *time.Duration {
	return &d
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	return n
}
