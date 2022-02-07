package statsd

import (
	"context"
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
	"github.com/atlassian/gostatsd/pb"
	"github.com/atlassian/gostatsd/pkg/transport"
)

func TestHttpForwarderV2Translation(t *testing.T) {
	t.Parallel()

	metrics := []*gostatsd.Metric{
		{
			Name:   "TestHttpForwarderTranslation.gauge",
			Value:  12345,
			Tags:   gostatsd.Tags{"TestHttpForwarderTranslation.gauge.tag1", "TestHttpForwarderTranslation.gauge.tag2"},
			Source: "TestHttpForwarderTranslation.gauge.host",
			Rate:   1,
			Type:   gostatsd.GAUGE,
		},
		{
			Name:   "TestHttpForwarderTranslation.gaugerate",
			Value:  12346,
			Tags:   gostatsd.Tags{"TestHttpForwarderTranslation.gaugerate.tag1", "TestHttpForwarderTranslation.gaugerate.tag2"},
			Source: "TestHttpForwarderTranslation.gaugerate.host",
			Rate:   0.1, // ignored
			Type:   gostatsd.GAUGE,
		},
		{
			Name:   "TestHttpForwarderTranslation.counter",
			Value:  12347,
			Tags:   gostatsd.Tags{"TestHttpForwarderTranslation.counter.tag1", "TestHttpForwarderTranslation.counter.tag2"},
			Source: "TestHttpForwarderTranslation.counter.host",
			Rate:   1,
			Type:   gostatsd.COUNTER,
		},
		{
			Name:   "TestHttpForwarderTranslation.counterrate",
			Value:  12348,
			Tags:   gostatsd.Tags{"TestHttpForwarderTranslation.counterrate.tag1", "TestHttpForwarderTranslation.counterrate.tag2"},
			Source: "TestHttpForwarderTranslation.counterrate.host",
			Rate:   0.1, // multiplied out
			Type:   gostatsd.COUNTER,
		},
		{
			Name:   "TestHttpForwarderTranslation.timer",
			Value:  12349,
			Tags:   gostatsd.Tags{"TestHttpForwarderTranslation.timer.tag1", "TestHttpForwarderTranslation.timer.tag2"},
			Source: "TestHttpForwarderTranslation.timer.host",
			Rate:   1,
			Type:   gostatsd.TIMER,
		},
		{
			Name:   "TestHttpForwarderTranslation.timerrate",
			Value:  12350,
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

func BenchmarkHttpForwarderV2TranslateAll(b *testing.B) {
	metrics := []*gostatsd.Metric{}

	for i := 0; i < 1000; i++ {
		metrics = append(metrics, &gostatsd.Metric{
			Name:        "bench.metric",
			Value:       123.456,
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

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		translateToProtobufV2(mm)
	}
}

func TestForwardingData(t *testing.T) {
	t.Parallel()
	var called, havePineapples, haveDerpinton uint64
	s := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		atomic.AddUint64(&called, 1)

		buf, err := io.ReadAll(r.Body)
		require.NoError(t, err, "Must not error when reading request")
		defer r.Body.Close()

		var data pb.RawMessageV2
		require.NoError(t, proto.Unmarshal(buf, &data))

		counters, ok := data.GetCounters()["pineapples"]
		if ok {
			atomic.AddUint64(&havePineapples, 1)
			val, ok := counters.GetTagMap()["derpinton"]
			if ok {
				atomic.AddUint64(&haveDerpinton, 1)
				assert.Equal(t, int64(10), val.GetValue())
			}
		}

	}))
	t.Cleanup(s.Close)

	log := logrus.New().WithField("testcase", t.Name())
	pool := transport.NewTransportPool(log, viper.New())
	forwarder, err := NewHttpForwarderHandlerV2(
		log,
		"default",
		s.URL,
		1,
		1,
		false,
		100*time.Millisecond, // maxRequestElapsedTime
		100*time.Millisecond, // flushInterval
		map[string]string{},
		[]string{},
		pool,
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

	assert.Greater(t, atomic.LoadUint64(&called), uint64(0), "Handler must have been called")
	assert.EqualValues(t, 1, atomic.LoadUint64(&havePineapples))
	assert.EqualValues(t, 1, atomic.LoadUint64(&haveDerpinton))
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
		h, err := NewHttpForwarderHandlerV2(logger, "default", "endpoint", 1, 1, false, time.Second, time.Second,
			cusHeaders, testcase.dynHeaders, pool)
		require.Nil(t, err)
		require.Equal(t, h.dynHeaderNames, testcase.expected)
	}
}
