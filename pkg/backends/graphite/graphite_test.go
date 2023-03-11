package graphite

import (
	"context"
	"io"
	"math"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ash2k/stager/wait"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atlassian/gostatsd"
)

func TestPreparePayloadLegacy(t *testing.T) {
	t.Parallel()
	metrics := metricsWithTags()
	expected := "stats_counts.stat1.gs 5 1234\n" +
		"stats.stat1.gs 1.100000 1234\n" +
		"stats_counts.stat1.gs 10 1234\n" +
		"stats.stat1.gs 2.200000 1234\n" +
		"stats_counts.stat1.gs 15 1234\n" +
		"stats.stat1.gs 3.300000 1234\n" +
		"stats_counts.stat1.gs 20 1234\n" +
		"stats.stat1.gs 4.400000 1234\n" +
		"stats.timers.t1.lower.gs 0.000000 1234\n" +
		"stats.timers.t1.upper.gs 0.000000 1234\n" +
		"stats.timers.t1.count.gs 0 1234\n" +
		"stats.timers.t1.count_ps.gs 0.000000 1234\n" +
		"stats.timers.t1.mean.gs 0.000000 1234\n" +
		"stats.timers.t1.median.gs 0.000000 1234\n" +
		"stats.timers.t1.std.gs 0.000000 1234\n" +
		"stats.timers.t1.sum.gs 0.000000 1234\n" +
		"stats.timers.t1.sum_squares.gs 0.000000 1234\n" +
		"stats.timers.t1.count_90.gs 90.000000 1234\n" +
		"stats.gauges.g1.gs 3.000000 1234\n" +
		"stats.sets.users.gs 3 1234\n"
	cl, err := NewClient("127.0.0.1:9", 1*time.Second, 1*time.Second, "ignored1", "ignored2", "ignored3", "ignored4", "ignored5", "gs", "legacy", gostatsd.TimerSubtypes{}, logrus.New())
	require.NoError(t, err)
	b := cl.preparePayload(metrics, time.Unix(1234, 0))
	expected = sortLines(expected)
	actual := sortLines(b.String())
	require.Equal(t, expected, actual)
}

func TestPreparePayloadBasic(t *testing.T) {
	t.Parallel()
	metrics := metricsWithTags()
	expected := "gp.pc.stat1.count.gs 5 1234\n" +
		"gp.pc.stat1.rate.gs 1.100000 1234\n" +
		"gp.pc.stat1.count.gs 10 1234\n" +
		"gp.pc.stat1.rate.gs 2.200000 1234\n" +
		"gp.pc.stat1.count.gs 15 1234\n" +
		"gp.pc.stat1.rate.gs 3.300000 1234\n" +
		"gp.pc.stat1.count.gs 20 1234\n" +
		"gp.pc.stat1.rate.gs 4.400000 1234\n" +
		"gp.pt.t1.lower.gs 0.000000 1234\n" +
		"gp.pt.t1.upper.gs 0.000000 1234\n" +
		"gp.pt.t1.count.gs 0 1234\n" +
		"gp.pt.t1.count_ps.gs 0.000000 1234\n" +
		"gp.pt.t1.mean.gs 0.000000 1234\n" +
		"gp.pt.t1.median.gs 0.000000 1234\n" +
		"gp.pt.t1.std.gs 0.000000 1234\n" +
		"gp.pt.t1.sum.gs 0.000000 1234\n" +
		"gp.pt.t1.sum_squares.gs 0.000000 1234\n" +
		"gp.pt.t1.count_90.gs 90.000000 1234\n" +
		"gp.pg.g1.gs 3.000000 1234\n" +
		"gp.ps.users.gs 3 1234\n"
	cl, err := NewClient("127.0.0.1:9", 1*time.Second, 1*time.Second, "gp", "pc", "pt", "pg", "ps", "gs", "basic", gostatsd.TimerSubtypes{}, logrus.New())
	require.NoError(t, err)
	b := cl.preparePayload(metrics, time.Unix(1234, 0))
	expected = sortLines(expected)
	actual := sortLines(b.String())
	require.Equal(t, expected, actual)
}

func TestPreparePayloadTags(t *testing.T) {
	t.Parallel()
	metrics := metricsWithTags()
	expected := "gp.pc.stat1.count.gs 5 1234\n" +
		"gp.pc.stat1.rate.gs 1.100000 1234\n" +
		"gp.pc.stat1.count.gs;unnamed=t 10 1234\n" +
		"gp.pc.stat1.rate.gs;unnamed=t 2.200000 1234\n" +
		"gp.pc.stat1.count.gs;k=v 15 1234\n" +
		"gp.pc.stat1.rate.gs;k=v 3.300000 1234\n" +
		"gp.pc.stat1.count.gs;k=v;unnamed=t 20 1234\n" +
		"gp.pc.stat1.rate.gs;k=v;unnamed=t 4.400000 1234\n" +
		"gp.pt.t1.lower.gs 0.000000 1234\n" +
		"gp.pt.t1.upper.gs 0.000000 1234\n" +
		"gp.pt.t1.count.gs 0 1234\n" +
		"gp.pt.t1.count_ps.gs 0.000000 1234\n" +
		"gp.pt.t1.mean.gs 0.000000 1234\n" +
		"gp.pt.t1.median.gs 0.000000 1234\n" +
		"gp.pt.t1.std.gs 0.000000 1234\n" +
		"gp.pt.t1.sum.gs 0.000000 1234\n" +
		"gp.pt.t1.sum_squares.gs 0.000000 1234\n" +
		"gp.pt.t1.count_90.gs 90.000000 1234\n" +
		"gp.pg.g1.gs 3.000000 1234\n" +
		"gp.ps.users.gs 3 1234\n"
	cl, err := NewClient("127.0.0.1:9", 1*time.Second, 1*time.Second, "gp", "pc", "pt", "pg", "ps", "gs", "tags", gostatsd.TimerSubtypes{}, logrus.New())
	require.NoError(t, err)
	b := cl.preparePayload(metrics, time.Unix(1234, 0))
	expected = sortLines(expected)
	actual := sortLines(b.String())
	require.Equal(t, expected, actual)
}

func TestPreparePayloadHistogram(t *testing.T) {
	t.Parallel()
	metrics := metricsWithHistogram()
	expected :=
		"gp.pc.t1.histogram.gs;le=20 5 1234\n" +
			"gp.pc.t1.histogram.gs;le=30 10 1234\n" +
			"gp.pc.t1.histogram.gs;le=40 10 1234\n" +
			"gp.pc.t1.histogram.gs;le=50 10 1234\n" +
			"gp.pc.t1.histogram.gs;le=60 19 1234\n" +
			"gp.pc.t1.histogram.gs;le=+Inf 19 1234\n"

	cl, err := NewClient("127.0.0.1:9", 1*time.Second, 1*time.Second, "gp", "pc", "pt", "pg", "ps", "gs", "tags", gostatsd.TimerSubtypes{}, logrus.New())
	require.NoError(t, err)
	b := cl.preparePayload(metrics, time.Unix(1234, 0))
	expected = sortLines(expected)
	actual := sortLines(b.String())
	require.Equal(t, expected, actual)
}

func sortLines(s string) string {
	lines := strings.Split(s, "\n")
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func TestSendMetricsAsync(t *testing.T) {
	t.Parallel()
	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer l.Close()
	addr := l.Addr().String()
	c, err := NewClient(addr, 1*time.Second, 10*time.Second, "", "", "", "", "", "", "basic", gostatsd.TimerSubtypes{}, logrus.New())
	require.NoError(t, err)

	var acceptWg sync.WaitGroup
	acceptWg.Add(1)
	go func() {
		defer acceptWg.Done()
		conn, e := l.Accept()
		if !assert.NoError(t, e) {
			return
		}
		defer conn.Close()
		d := make([]byte, 1024)
		for {
			assert.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
			_, e := conn.Read(d)
			if e == io.EOF {
				break
			}
			assert.NoError(t, e)
		}
	}()
	defer acceptWg.Wait()

	var wg wait.Group
	defer wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	wg.StartWithContext(ctx, c.Run)
	var swg sync.WaitGroup
	swg.Add(1)
	c.SendMetricsAsync(ctx, metrics(), func(errs []error) {
		defer swg.Done()
		for i, e := range errs {
			assert.NoError(t, e, i)
		}
	})
	swg.Wait()
}

func metrics() *gostatsd.MetricMap {
	timestamp := gostatsd.Nanotime(time.Unix(123456, 0).UnixNano())

	mm := gostatsd.NewMetricMap(false)
	mm.Counters["stat1"] = map[string]gostatsd.Counter{}
	mm.Counters["stat1"][""] = gostatsd.Counter{PerSecond: 1.1, Value: 5, Timestamp: timestamp}
	mm.Timers["t1"] = map[string]gostatsd.Timer{}
	mm.Timers["t1"][""] = gostatsd.Timer{Values: []float64{10}, Percentiles: gostatsd.Percentiles{gostatsd.Percentile{Float: 90, Str: "count_90"}}, Timestamp: timestamp}
	mm.Gauges["g1"] = map[string]gostatsd.Gauge{}
	mm.Gauges["g1"][""] = gostatsd.Gauge{Value: 3, Timestamp: timestamp}
	mm.Sets["users"] = map[string]gostatsd.Set{}
	mm.Sets["users"][""] = gostatsd.Set{Values: map[string]struct{}{"joe": {}, "bob": {}, "john": {}}, Timestamp: timestamp}
	return mm
}

func metricsWithHistogram() *gostatsd.MetricMap {
	timestamp := gostatsd.Nanotime(time.Unix(123456, 0).UnixNano())

	mm := gostatsd.NewMetricMap(false)
	mm.Timers["t1"] = map[string]gostatsd.Timer{}
	mm.Timers["t1"]["gsd_histogram:20_30_40_50_60"] = gostatsd.Timer{Values: []float64{10}, Timestamp: timestamp, Histogram: map[gostatsd.HistogramThreshold]int{
		20:                                       5,
		30:                                       10,
		40:                                       10,
		50:                                       10,
		60:                                       19,
		gostatsd.HistogramThreshold(math.Inf(1)): 19,
	}}
	return mm
}

func metricsWithTags() *gostatsd.MetricMap {
	timestamp := gostatsd.Nanotime(time.Unix(123456, 0).UnixNano())

	m := metrics()
	m.Counters["stat1"]["t"] = gostatsd.Counter{PerSecond: 2.2, Value: 10, Timestamp: timestamp, Tags: gostatsd.Tags{"t"}}
	m.Counters["stat1"]["k:v"] = gostatsd.Counter{PerSecond: 3.3, Value: 15, Timestamp: timestamp, Tags: gostatsd.Tags{"k:v"}}
	m.Counters["stat1"]["k:v.t"] = gostatsd.Counter{PerSecond: 4.4, Value: 20, Timestamp: timestamp, Tags: gostatsd.Tags{"k:v", "t"}}
	return m
}
