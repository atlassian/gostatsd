package graphite

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"time"

	backendTypes "github.com/atlassian/gostatsd/backend/types"
	"github.com/atlassian/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

const (
	// BackendName is the name of this backend.
	BackendName         = "graphite"
	defaultDialTimeout  = 5 * time.Second
	defaultWriteTimeout = 30 * time.Second
)

const sampleConfig = `
[graphite]
	# graphite host or ip address
	address = "ip:2003"
`

// normalizeBucketName cleans up a bucket name by replacing or translating invalid characters.
func normalizeBucketName(bucket string, tagsKey string) string {
	tags := strings.Split(tagsKey, ",")
	for _, tag := range tags {
		if tag != "" {
			bucket += "." + types.TagToMetricName(tag)
		}
	}
	return bucket
}

// client is an object that is used to send messages to a Graphite server's TCP interface.
type client struct {
	address      string
	dialTimeout  time.Duration
	writeTimeout time.Duration
}

// SendMetricsAsync flushes the metrics to the Graphite server, preparing payload synchronously but doing the send asynchronously.
func (client *client) SendMetricsAsync(ctx context.Context, metrics *types.MetricMap, cb backendTypes.SendCallback) {
	if metrics.NumStats == 0 {
		cb(nil)
		return
	}
	buf := preparePayload(metrics)
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

func preparePayload(metrics *types.MetricMap) *bytes.Buffer {
	buf := new(bytes.Buffer)
	now := time.Now().Unix()
	metrics.Counters.Each(func(key, tagsKey string, counter types.Counter) {
		nk := normalizeBucketName(key, tagsKey)
		fmt.Fprintf(buf, "stats_count.%s %f %d\n", nk, float64(counter.Value), now)
		fmt.Fprintf(buf, "stats.%s %f %d\n", nk, counter.PerSecond, now)
	})
	metrics.Timers.Each(func(key, tagsKey string, timer types.Timer) {
		nk := normalizeBucketName(key, tagsKey)
		fmt.Fprintf(buf, "stats.timers.%s.lower %f %d\n", nk, timer.Min, now)
		fmt.Fprintf(buf, "stats.timers.%s.upper %f %d\n", nk, timer.Max, now)
		fmt.Fprintf(buf, "stats.timers.%s.count %f %d\n", nk, timer.Count, now)
		fmt.Fprintf(buf, "stats.timers.%s.count_ps %f %d\n", nk, timer.PerSecond, now)
		fmt.Fprintf(buf, "stats.timers.%s.mean %f %d\n", nk, timer.Mean, now)
		fmt.Fprintf(buf, "stats.timers.%s.median %f %d\n", nk, timer.Median, now)
		fmt.Fprintf(buf, "stats.timers.%s.sum %f %d\n", nk, timer.Sum, now)
		fmt.Fprintf(buf, "stats.timers.%s.sum %f %d\n", nk, timer.SumSquares, now)
		fmt.Fprintf(buf, "stats.timers.%s.sum_squares %f %d\n", nk, timer.StdDev, now)
		for _, pct := range timer.Percentiles {
			fmt.Fprintf(buf, "stats.timers.%s.%s %f %d\n", nk, pct.String(), pct.Float(), now)
		}
	})
	metrics.Gauges.Each(func(key, tagsKey string, gauge types.Gauge) {
		nk := normalizeBucketName(key, tagsKey)
		fmt.Fprintf(buf, "stats.gauge.%s %f %d\n", nk, gauge.Value, now)
	})
	metrics.Sets.Each(func(key, tagsKey string, set types.Set) {
		nk := normalizeBucketName(key, tagsKey)
		fmt.Fprintf(buf, "stats.sets.%s %d %d\n", nk, len(set.Values), now)
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

// NewClientFromViper constructs a GraphiteClient object by connecting to an address.
func NewClientFromViper(v *viper.Viper) (backendTypes.Backend, error) {
	g := getSubViper(v, "graphite")
	g.SetDefault("address", "localhost:2003")
	g.SetDefault("dial_timeout", defaultDialTimeout)
	g.SetDefault("write_timeout", defaultWriteTimeout)
	return NewClient(
		g.GetString("address"),
		g.GetDuration("dial_timeout"),
		g.GetDuration("write_timeout"),
	)
}

// NewClient constructs a Graphite backend object.
func NewClient(address string, dialTimeout, writeTimeout time.Duration) (backendTypes.Backend, error) {
	if address == "" {
		return nil, fmt.Errorf("[%s] address is required", BackendName)
	}
	if dialTimeout <= 0 {
		return nil, fmt.Errorf("[%s] dialTimeout should be positive", BackendName)
	}
	if writeTimeout < 0 {
		return nil, fmt.Errorf("[%s] writeTimeout should be non-negative", BackendName)
	}
	log.Infof("[%s] address=%s dialTimeout=%s writeTimeout=%s", BackendName, address, dialTimeout, writeTimeout)
	return &client{
		address:      address,
		dialTimeout:  dialTimeout,
		writeTimeout: writeTimeout,
	}, nil
}

// BackendName returns the name of the backend.
func (client *client) BackendName() string {
	return BackendName
}

// Workaround https://github.com/spf13/viper/pull/165 and https://github.com/spf13/viper/issues/191
func getSubViper(v *viper.Viper, key string) *viper.Viper {
	var n *viper.Viper
	namespace := v.Get(key)
	if namespace != nil {
		n = v.Sub(key)
	}
	if n == nil {
		n = viper.New()
	}
	return n
}
