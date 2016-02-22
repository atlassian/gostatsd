package graphite

import (
	"bytes"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/jtblin/gostatsd/backend"
	"github.com/jtblin/gostatsd/types"

	"github.com/spf13/viper"
)

const backendName = "graphite"

func init() {
	viper.SetDefault("graphite.address", "localhost:2003")
	backend.RegisterBackend(backendName, func() (backend.MetricSender, error) {
		return NewGraphiteClient()
	})
}

const sampleConfig = `
[graphite]
	# graphite host or ip address
	address = "ip:2003"
`

// Regular expressions used for bucket name normalization
var regSemiColon = regexp.MustCompile(":")

// normalizeBucketName cleans up a bucket name by replacing or translating invalid characters
func normalizeBucketName(bucket string, tagsKey string) string {
	tags := strings.Split(tagsKey, ",")
	for _, tag := range tags {
		bucket += "." + regSemiColon.ReplaceAllString(tag, "_")
	}
	return bucket
}

// GraphiteClient is an object that is used to send messages to a Graphite server's UDP interface
type GraphiteClient struct {
	conn *net.Conn
}

// SendMetrics sends the metrics in a MetricsMap to the Graphite server
func (client *GraphiteClient) SendMetrics(metrics types.MetricMap) error {
	if metrics.NumStats == 0 {
		return nil
	}
	buf := new(bytes.Buffer)
	now := time.Now().Unix()
	types.EachCounter(metrics.Counters, func(key, tagsKey string, counter types.Counter) {
		nk := normalizeBucketName(key, tagsKey)
		fmt.Fprintf(buf, "stats_count.%s %d %d\n", nk, counter.Value, now)
		fmt.Fprintf(buf, "stats.%s %f %d\n", nk, counter.PerSecond, now)
	})
	types.EachTimer(metrics.Timers, func(key, tagsKey string, timer types.Timer) {
		nk := normalizeBucketName(key, tagsKey)
		fmt.Fprintf(buf, "stats.timers.%s.lower %f %d\n", nk, timer.Min, now)
		fmt.Fprintf(buf, "stats.timers.%s.upper %f %d\n", nk, timer.Max, now)
		fmt.Fprintf(buf, "stats.timers.%s.count %f %d\n", nk, timer.Count, now)
	})
	types.EachGauge(metrics.Gauges, func(key, tagsKey string, gauge types.Gauge) {
		nk := normalizeBucketName(key, tagsKey)
		fmt.Fprintf(buf, "stats.gauge.%s %f %d\n", nk, gauge.Value, now)
	})

	types.EachSet(metrics.Sets, func(key, tagsKey string, set types.Set) {
		nk := normalizeBucketName(key, tagsKey)
		fmt.Fprintf(buf, "stats.sets.%s %d %d\n", nk, len(set.Values), now)
	})

	fmt.Fprintf(buf, "statsd.numStats %d %d\n", metrics.NumStats, now)

	_, err := buf.WriteTo(*client.conn)
	if err != nil {
		return fmt.Errorf("error sending to graphite: %s", err)
	}
	return nil
}

// SampleConfig returns the sample config for the graphite backend
func (g *GraphiteClient) SampleConfig() string {
	return sampleConfig
}

// NewGraphiteClient constructs a GraphiteClient object by connecting to an address
func NewGraphiteClient() (backend.MetricSender, error) {
	conn, err := net.Dial("tcp", viper.GetString("graphite.address"))
	if err != nil {
		return nil, err
	}
	return &GraphiteClient{&conn}, nil
}

func (client *GraphiteClient) Name() string {
	return backendName
}
