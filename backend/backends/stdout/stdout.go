package stdout

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jtblin/gostatsd/backend"
	"github.com/jtblin/gostatsd/types"

	log "github.com/Sirupsen/logrus"
)

const backendName = "stdout"

func init() {
	backend.RegisterBackend(backendName, func() (backend.MetricSender, error) {
		return NewStdoutClient()
	})
}

// StdoutClient is an object that is used to send messages to stdout
type StdoutClient struct{}

// NewStdoutClient constructs a StdoutClient object
func NewStdoutClient() (backend.MetricSender, error) {
	return &StdoutClient{}, nil
}

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

// SampleConfig returns the sample config for the stdout backend
func (s *StdoutClient) SampleConfig() string {
	return ""
}

// SendMetrics sends the metrics in a MetricsMap to the Graphite server
func (client *StdoutClient) SendMetrics(metrics types.MetricMap) error {
	buf := new(bytes.Buffer)
	now := time.Now().Unix()
	types.EachCounter(metrics.Counters, func(key, tagsKey string, counter types.Counter) {
		nk := normalizeBucketName(key, tagsKey)
		fmt.Fprintf(buf, "stats.counter.%s.count %d %d\n", nk, counter.Value, now)
		fmt.Fprintf(buf, "stats.counter.%s.per_second %f %d\n", nk, counter.PerSecond, now)
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
	fmt.Fprintf(buf, "statsd.numStats %d %d\n", metrics.NumStats, now)

	_, err := buf.WriteTo(log.StandardLogger().Writer())
	if err != nil {
		return err
	}
	return nil
}

func (client *StdoutClient) Name() string {
	return backendName
}
