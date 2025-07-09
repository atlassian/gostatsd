package statsd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ash2k/stager/wait"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tilinna/clock"
	"google.golang.org/protobuf/proto"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/fixtures"
	"github.com/atlassian/gostatsd/internal/flush"
	"github.com/atlassian/gostatsd/pb"
	"github.com/atlassian/gostatsd/pkg/healthcheck"
	"github.com/atlassian/gostatsd/pkg/transport"
	"github.com/atlassian/gostatsd/pkg/web"
)

type testServer struct {
	s              *httptest.Server
	called         uint64
	pineappleCount uint64
	derpCount      uint64
	derpValue      int64
}

func newTestServer(tb *testing.T) *testServer {
	t := &testServer{}
	t.s = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint64(&t.called, 1)

		buf, err := io.ReadAll(r.Body)
		require.NoError(tb, err, "Must not error when reading request")

		var data pb.RawMessageV2
		require.NoError(tb, proto.Unmarshal(buf, &data))

		counters, ok := data.GetCounters()["pineapples"]
		if ok {
			atomic.AddUint64(&t.pineappleCount, 1)
			val, ok := counters.GetTagMap()["derpinton"]
			if ok {
				atomic.AddUint64(&t.derpCount, 1)
				t.derpValue = val.GetValue()
			}
		}
	}))
	return t
}

func TestHttpForwarderDeepCheck(t *testing.T) {
	t.Parallel()

	logger := logrus.New()
	s := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(s.Close)
	hfh, err := NewHttpForwarderHandlerV2(
		logger,
		"",
		s.URL,
		1,
		1,
		1,
		false,
		"",
		0,
		1*time.Second,
		1*time.Second,
		nil,
		nil,
		transport.NewTransportPool(logger, viper.New()),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, hfh)

	deepChecks := hfh.DeepChecks()
	assert.Len(t, deepChecks, 1)

	msg, isHealthy := deepChecks[0]()
	t.Logf("%s\n", msg)
	assert.Equal(t, healthcheck.Unhealthy, isHealthy)

	hfh.sendNop(context.Background())

	msg, isHealthy = deepChecks[0]()
	t.Logf("%s\n", msg)
	assert.Equal(t, healthcheck.Healthy, isHealthy)
}

func TestHttpForwarderV2Translation(t *testing.T) {
	t.Parallel()

	metrics := []*gostatsd.Metric{
		{
			Name:   "TestHttpForwarderTranslation.gauge",
			Values: []float64{12345},
			Tags:   gostatsd.Tags{"TestHttpForwarderTranslation.gauge.tag1", "TestHttpForwarderTranslation.gauge.tag2"},
			Source: "TestHttpForwarderTranslation.gauge.host",
			Rate:   1,
			Type:   gostatsd.GAUGE,
		},
		{
			Name:   "TestHttpForwarderTranslation.gaugerate",
			Values: []float64{12346},
			Tags:   gostatsd.Tags{"TestHttpForwarderTranslation.gaugerate.tag1", "TestHttpForwarderTranslation.gaugerate.tag2"},
			Source: "TestHttpForwarderTranslation.gaugerate.host",
			Rate:   0.1, // ignored
			Type:   gostatsd.GAUGE,
		},
		{
			Name:   "TestHttpForwarderTranslation.counter",
			Values: []float64{12347},
			Tags:   gostatsd.Tags{"TestHttpForwarderTranslation.counter.tag1", "TestHttpForwarderTranslation.counter.tag2"},
			Source: "TestHttpForwarderTranslation.counter.host",
			Rate:   1,
			Type:   gostatsd.COUNTER,
		},
		{
			Name:   "TestHttpForwarderTranslation.counterrate",
			Values: []float64{12348},
			Tags:   gostatsd.Tags{"TestHttpForwarderTranslation.counterrate.tag1", "TestHttpForwarderTranslation.counterrate.tag2"},
			Source: "TestHttpForwarderTranslation.counterrate.host",
			Rate:   0.1, // multiplied out
			Type:   gostatsd.COUNTER,
		},
		{
			Name:   "TestHttpForwarderTranslation.timer",
			Values: []float64{12349},
			Tags:   gostatsd.Tags{"TestHttpForwarderTranslation.timer.tag1", "TestHttpForwarderTranslation.timer.tag2"},
			Source: "TestHttpForwarderTranslation.timer.host",
			Rate:   1,
			Type:   gostatsd.TIMER,
		},
		{
			Name:   "TestHttpForwarderTranslation.timerrate",
			Values: []float64{12350},
			Tags:   gostatsd.Tags{"TestHttpForwarderTranslation.timerrate.tag1", "TestHttpForwarderTranslation.timerrate.tag2"},
			Source: "TestHttpForwarderTranslation.timerrate.host",
			Rate:   0.1, // propagated
			Type:   gostatsd.TIMER,
		},
		{
			Name:        "TestHttpForwarderTranslation.set",
			StringValue: "12351",
			Tags:        gostatsd.Tags{"TestHttpForwarderTranslation.set.tag1", "TestHttpForwarderTranslation.set.tag2"},
			Source:      "TestHttpForwarderTranslation.set.host",
			Rate:        1,
			Type:        gostatsd.SET,
		},
		{
			Name:        "TestHttpForwarderTranslation.setrate",
			StringValue: "12352",
			Tags:        gostatsd.Tags{"TestHttpForwarderTranslation.setrate.tag1", "TestHttpForwarderTranslation.setrate.tag2"},
			Source:      "TestHttpForwarderTranslation.setrate.host",
			Rate:        0.1, // ignored
			Type:        gostatsd.SET,
		},
	}

	mm := gostatsd.NewMetricMap(false)
	for _, metric := range metrics {
		mm.Receive(metric)
	}

	pbMetrics := translateToProtobufV2(mm)

	expected := &pb.RawMessageV2{
		Gauges: map[string]*pb.GaugeTagV2{
			"TestHttpForwarderTranslation.gauge": {
				TagMap: map[string]*pb.RawGaugeV2{
					"TestHttpForwarderTranslation.gauge.tag1,TestHttpForwarderTranslation.gauge.tag2,s:TestHttpForwarderTranslation.gauge.host": {
						Tags:     []string{"TestHttpForwarderTranslation.gauge.tag1", "TestHttpForwarderTranslation.gauge.tag2"},
						Hostname: "TestHttpForwarderTranslation.gauge.host",
						Value:    12345,
					},
				},
			},
			"TestHttpForwarderTranslation.gaugerate": {
				TagMap: map[string]*pb.RawGaugeV2{
					"TestHttpForwarderTranslation.gaugerate.tag1,TestHttpForwarderTranslation.gaugerate.tag2,s:TestHttpForwarderTranslation.gaugerate.host": {
						Tags:     []string{"TestHttpForwarderTranslation.gaugerate.tag1", "TestHttpForwarderTranslation.gaugerate.tag2"},
						Hostname: "TestHttpForwarderTranslation.gaugerate.host",
						Value:    12346,
					},
				},
			},
		},
		Counters: map[string]*pb.CounterTagV2{
			"TestHttpForwarderTranslation.counter": {
				TagMap: map[string]*pb.RawCounterV2{
					"TestHttpForwarderTranslation.counter.tag1,TestHttpForwarderTranslation.counter.tag2,s:TestHttpForwarderTranslation.counter.host": {
						Tags:     []string{"TestHttpForwarderTranslation.counter.tag1", "TestHttpForwarderTranslation.counter.tag2"},
						Hostname: "TestHttpForwarderTranslation.counter.host",
						Value:    12347,
					},
				},
			},
			"TestHttpForwarderTranslation.counterrate": {
				TagMap: map[string]*pb.RawCounterV2{
					"TestHttpForwarderTranslation.counterrate.tag1,TestHttpForwarderTranslation.counterrate.tag2,s:TestHttpForwarderTranslation.counterrate.host": {
						Tags:     []string{"TestHttpForwarderTranslation.counterrate.tag1", "TestHttpForwarderTranslation.counterrate.tag2"},
						Hostname: "TestHttpForwarderTranslation.counterrate.host",
						Value:    123480, // rate is multipled out
					},
				},
			},
		},
		Timers: map[string]*pb.TimerTagV2{
			"TestHttpForwarderTranslation.timer": {
				TagMap: map[string]*pb.RawTimerV2{
					"TestHttpForwarderTranslation.timer.tag1,TestHttpForwarderTranslation.timer.tag2,s:TestHttpForwarderTranslation.timer.host": {
						Tags:        []string{"TestHttpForwarderTranslation.timer.tag1", "TestHttpForwarderTranslation.timer.tag2"},
						Hostname:    "TestHttpForwarderTranslation.timer.host",
						SampleCount: 1,
						Values:      []float64{12349},
					},
				},
			},
			"TestHttpForwarderTranslation.timerrate": {
				TagMap: map[string]*pb.RawTimerV2{
					"TestHttpForwarderTranslation.timerrate.tag1,TestHttpForwarderTranslation.timerrate.tag2,s:TestHttpForwarderTranslation.timerrate.host": {
						Tags:        []string{"TestHttpForwarderTranslation.timerrate.tag1", "TestHttpForwarderTranslation.timerrate.tag2"},
						Hostname:    "TestHttpForwarderTranslation.timerrate.host",
						SampleCount: 10,
						Values:      []float64{12350},
					},
				},
			},
		},
		Sets: map[string]*pb.SetTagV2{
			"TestHttpForwarderTranslation.set": {
				TagMap: map[string]*pb.RawSetV2{
					"TestHttpForwarderTranslation.set.tag1,TestHttpForwarderTranslation.set.tag2,s:TestHttpForwarderTranslation.set.host": {
						Tags:     []string{"TestHttpForwarderTranslation.set.tag1", "TestHttpForwarderTranslation.set.tag2"},
						Hostname: "TestHttpForwarderTranslation.set.host",
						Values:   []string{"12351"},
					},
				},
			},
			"TestHttpForwarderTranslation.setrate": {
				TagMap: map[string]*pb.RawSetV2{
					"TestHttpForwarderTranslation.setrate.tag1,TestHttpForwarderTranslation.setrate.tag2,s:TestHttpForwarderTranslation.setrate.host": {
						Tags:     []string{"TestHttpForwarderTranslation.setrate.tag1", "TestHttpForwarderTranslation.setrate.tag2"},
						Hostname: "TestHttpForwarderTranslation.setrate.host",
						Values:   []string{"12352"},
					},
				},
			},
		},
	}
	//require.EqualValues(t, expected, pbMetrics)
	require.EqualValues(t, expected.Gauges, pbMetrics.Gauges)
	require.EqualValues(t, expected.Counters, pbMetrics.Counters)
	require.EqualValues(t, expected.Timers, pbMetrics.Timers)
	require.EqualValues(t, expected.Sets, pbMetrics.Sets)
}

// Get a large MetricMap useful for benchmarking
func createMetricMapTestFixture() *gostatsd.MetricMap {
	metrics := []*gostatsd.Metric{}

	for i := 0; i < 1000; i++ {
		metrics = append(metrics, &gostatsd.Metric{
			Name:        "bench.metric",
			Values:      []float64{123.456},
			StringValue: "123.456",
			Tags:        gostatsd.Tags{"tag1", "tag2"},
			Source:      "hostname",
			Timestamp:   10,
			Type:        1 + gostatsd.MetricType(i%4), // Use all types
		})
	}
	mm := gostatsd.NewMetricMap(false)
	for _, metric := range metrics {
		mm.Receive(metric)
	}

	return mm
}

func BenchmarkHttpForwarderV2TranslateAll(b *testing.B) {
	mm := createMetricMapTestFixture()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		translateToProtobufV2(mm)
	}
}

func BenchmarkHttpForwarderV2Compression_Zlib(b *testing.B) {
	message := translateToProtobufV2(createMetricMapTestFixture())
	raw, _ := proto.Marshal(message)

	b.ReportAllocs()

	for compressionLevel := 0; compressionLevel < 10; compressionLevel++ {
		b.Run(fmt.Sprintf("zlib_compression_level_%d", compressionLevel), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				buf := &bytes.Buffer{}
				web.CompressWithZlib(raw, buf, compressionLevel)
			}
		})
	}
}

func BenchmarkHttpForwarderV2Compression_Lz4(b *testing.B) {
	message := translateToProtobufV2(createMetricMapTestFixture())
	raw, _ := proto.Marshal(message)

	b.ReportAllocs()

	for compressionLevel := 0; compressionLevel < 10; compressionLevel++ {
		b.Run(fmt.Sprintf("lz4_compression_level_%d", compressionLevel), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				buf := &bytes.Buffer{}
				web.CompressWithLz4(raw, buf, compressionLevel)
			}
		})
	}
}

func TestHttpForwarderV2Compression_CompressionRatio(t *testing.T) {
	message := translateToProtobufV2(createMetricMapTestFixture())
	raw, _ := proto.Marshal(message)

	for compressionLevel := 0; compressionLevel < 10; compressionLevel++ {
		buf := &bytes.Buffer{}
		web.CompressWithZlib(raw, buf, compressionLevel)
		assert.Greater(t, buf.Len(), 0, "Compressed size should not be zero")
		fmt.Printf("zlib level %v: %.3f%%\n", compressionLevel, 100*float64(buf.Len())/float64(len(raw)))
	}

	for compressionLevel := 0; compressionLevel < 10; compressionLevel++ {
		buf := &bytes.Buffer{}
		web.CompressWithLz4(raw, buf, compressionLevel)
		assert.Greater(t, buf.Len(), 0, "Compressed size should not be zero")
		fmt.Printf("lz4 level %v: %.3f%%\n", compressionLevel, 100*float64(buf.Len())/float64(len(raw)))
	}
}

func TestForwardingData(t *testing.T) {
	t.Parallel()

	ts := newTestServer(t)
	t.Cleanup(ts.s.Close)

	log := logrus.New().WithField("testcase", t.Name())
	pool := transport.NewTransportPool(log, viper.New())
	forwarder, err := NewHttpForwarderHandlerV2(
		log,
		"default",
		ts.s.URL,
		1,
		1,
		1,
		false,
		"",
		0,
		100*time.Millisecond, // maxRequestElapsedTime
		100*time.Millisecond, // flushInterval
		map[string]string{},
		[]string{},
		pool,
		nil,
	)
	require.NoError(t, err, "Must not error when creating the forwarder")

	ctx, cancel := context.WithCancel(context.Background())
	mockClock := clock.NewMock(time.Unix(1000, 0))
	ctx = clock.Context(ctx, mockClock)
	t.Cleanup(cancel)

	for i := 0; i < 10; i++ {
		forwarder.DispatchMetricMap(ctx, &gostatsd.MetricMap{
			Counters: gostatsd.Counters{
				"pineapples": map[string]gostatsd.Counter{
					"derpinton": {
						PerSecond: 0.1,
						Value:     1,
						Source:    gostatsd.UnknownSource,
						Timestamp: gostatsd.Nanotime(mockClock.Now().Nanosecond()),
						Tags:      gostatsd.Tags{},
					},
				},
			},
		})
	}
	var wg wait.Group
	wg.StartWithContext(ctx, forwarder.Run)
	fixtures.NextStep(ctx, mockClock)

	cancel()

	wg.Wait()

	assert.Greater(t, atomic.LoadUint64(&ts.called), uint64(0), "Handler must have been called")
	assert.EqualValues(t, 1, atomic.LoadUint64(&ts.pineappleCount))
	assert.EqualValues(t, 1, atomic.LoadUint64(&ts.derpCount))
	assert.EqualValues(t, 10, atomic.LoadInt64(&ts.derpValue))
	assert.Equal(t, 0, mockClock.Len(), "Must have closed all event handlers")
}

func TestHttpForwarderV2New(t *testing.T) {
	logger := logrus.New()
	pool := transport.NewTransportPool(logger, viper.New())
	cusHeaders := map[string]string{"region": "us", "env": "dev"}

	for _, testcase := range []struct {
		dynHeaders []string
		expected   []string
	}{
		{
			dynHeaders: []string{"service", "deploy"},
			expected:   []string{"service:", "deploy:"},
		},
		{
			dynHeaders: []string{"service", "deploy", "env"},
			expected:   []string{"service:", "deploy:"},
		},
	} {
		h, err := NewHttpForwarderHandlerV2(logger, "default", "endpoint", 1, 1, 1, false, "", 0, time.Second, time.Second,
			cusHeaders, testcase.dynHeaders, pool, nil)
		require.Nil(t, err)
		require.Equal(t, h.dynHeaderNames, testcase.expected)
	}
}

func TestManualFlush(t *testing.T) {
	t.Parallel()

	ts := newTestServer(t)
	t.Cleanup(ts.s.Close)

	log := logrus.New().WithField("testcase", t.Name())
	pool := transport.NewTransportPool(log, viper.New())
	fc := flush.NewFlushCoordinator()
	forwarder, err := NewHttpForwarderHandlerV2(
		log,
		"default",
		ts.s.URL,
		1,
		1,
		1,
		false,
		"",
		0,
		100*time.Millisecond, // maxRequestElapsedTime
		100*time.Millisecond, // flushInterval
		map[string]string{},
		[]string{},
		pool,
		fc,
	)
	require.NoError(t, err, "Must not error when creating the forwarder")

	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < 10; i++ {
		forwarder.DispatchMetricMap(ctx, &gostatsd.MetricMap{
			Counters: gostatsd.Counters{
				"pineapples": map[string]gostatsd.Counter{
					"derpinton": {
						PerSecond: 0.1,
						Value:     1,
						Source:    gostatsd.UnknownSource,
						Timestamp: gostatsd.Nanotime(time.Now().Nanosecond()),
						Tags:      gostatsd.Tags{},
					},
				},
			},
		})
	}

	var wg wait.Group
	wg.StartWithContext(ctx, forwarder.Run)

	fc.Flush()
	fc.WaitForFlush()

	cancel()

	wg.Wait()

	// First is for the "nop" (see HttpForwarderHandlerV2.Run + HttpForwarderHandlerV2.sendNop)
	// Second is for the actual flush.
	assert.Equal(t, uint64(1+1), atomic.LoadUint64(&ts.called), "Handler must have been called")
	assert.EqualValues(t, 1, atomic.LoadUint64(&ts.pineappleCount))
	assert.EqualValues(t, 1, atomic.LoadUint64(&ts.derpCount))
	assert.EqualValues(t, 10, atomic.LoadInt64(&ts.derpValue))
}

func TestViperMerges(t *testing.T) {
	t.Parallel()

	overrides := viper.New()
	overrides.SetDefault("http-transport.api-endpoint", "localhost")

	values := newHTTPForwarderHandlerViperConfig(overrides)
	assert.Equal(
		t,
		map[string]any{
			"transport":                defaultTransport,
			"compress":                 defaultCompress,
			"compression-type":         defaultCompressionType,
			"compression-level":        defaultCompressionLevel,
			"api-endpoint":             "localhost",
			"max-requests":             defaultMaxRequests,
			"max-request-elapsed-time": defaultMaxRequestElapsedTime,
			"consolidator-slots":       gostatsd.DefaultMaxParsers,
			"flush-interval":           defaultConsolidatorFlushInterval,
			"concurrent-merge":         defaultConcurrentMerge,
		},
		values.AllSettings(),
	)
}
