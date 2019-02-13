package statsd

import (
	"testing"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pb"
	"github.com/stretchr/testify/require"
)

func TestHttpForwarderTranslation(t *testing.T) {
	t.Parallel()
	metrics := []*gostatsd.Metric{
		{
			Name:     "TestHttpForwarderTranslation.gauge",
			Value:    12345,
			Tags:     gostatsd.Tags{"TestHttpForwarderTranslation.gauge.tag1", "TestHttpForwarderTranslation.gauge.tag2"},
			Hostname: "TestHttpForwarderTranslation.gauge.host",
			Rate:     1,
			Type:     gostatsd.GAUGE,
		},
		{
			Name:     "TestHttpForwarderTranslation.gaugerate",
			Value:    12346,
			Tags:     gostatsd.Tags{"TestHttpForwarderTranslation.gaugerate.tag1", "TestHttpForwarderTranslation.gaugerate.tag2"},
			Hostname: "TestHttpForwarderTranslation.gaugerate.host",
			Rate:     0.1, // ignored
			Type:     gostatsd.GAUGE,
		},
		{
			Name:     "TestHttpForwarderTranslation.counter",
			Value:    12347,
			Tags:     gostatsd.Tags{"TestHttpForwarderTranslation.counter.tag1", "TestHttpForwarderTranslation.counter.tag2"},
			Hostname: "TestHttpForwarderTranslation.counter.host",
			Rate:     1,
			Type:     gostatsd.COUNTER,
		},
		{
			Name:     "TestHttpForwarderTranslation.counterrate",
			Value:    12348,
			Tags:     gostatsd.Tags{"TestHttpForwarderTranslation.counterrate.tag1", "TestHttpForwarderTranslation.counterrate.tag2"},
			Hostname: "TestHttpForwarderTranslation.counterrate.host",
			Rate:     0.1, // multiplied out
			Type:     gostatsd.COUNTER,
		},
		{
			Name:     "TestHttpForwarderTranslation.timer",
			Value:    12349,
			Tags:     gostatsd.Tags{"TestHttpForwarderTranslation.timer.tag1", "TestHttpForwarderTranslation.timer.tag2"},
			Hostname: "TestHttpForwarderTranslation.timer.host",
			Rate:     1,
			Type:     gostatsd.TIMER,
		},
		{
			Name:     "TestHttpForwarderTranslation.timerrate",
			Value:    12350,
			Tags:     gostatsd.Tags{"TestHttpForwarderTranslation.timerrate.tag1", "TestHttpForwarderTranslation.timerrate.tag2"},
			Hostname: "TestHttpForwarderTranslation.timerrate.host",
			Rate:     0.1, // propagated
			Type:     gostatsd.TIMER,
		},
		{
			Name:        "TestHttpForwarderTranslation.set",
			StringValue: "12351",
			Tags:        gostatsd.Tags{"TestHttpForwarderTranslation.set.tag1", "TestHttpForwarderTranslation.set.tag2"},
			Hostname:    "TestHttpForwarderTranslation.set.host",
			Rate:        1,
			Type:        gostatsd.SET,
		},
		{
			Name:        "TestHttpForwarderTranslation.setrate",
			StringValue: "12352",
			Tags:        gostatsd.Tags{"TestHttpForwarderTranslation.setrate.tag1", "TestHttpForwarderTranslation.setrate.tag2"},
			Hostname:    "TestHttpForwarderTranslation.setrate.host",
			Rate:        0.1, // ignored
			Type:        gostatsd.SET,
		},
	}

	pbMetrics := translateToProtobuf(metrics)

	expected := []*pb.RawMetricV1{
		{
			Name:     "TestHttpForwarderTranslation.gauge",
			Tags:     []string{"TestHttpForwarderTranslation.gauge.tag1", "TestHttpForwarderTranslation.gauge.tag2"},
			Hostname: "TestHttpForwarderTranslation.gauge.host",
			M: &pb.RawMetricV1_Gauge{
				Gauge: &pb.RawGaugeV1{
					Value: 12345,
				},
			},
		},
		{
			Name:     "TestHttpForwarderTranslation.gaugerate",
			Tags:     []string{"TestHttpForwarderTranslation.gaugerate.tag1", "TestHttpForwarderTranslation.gaugerate.tag2"},
			Hostname: "TestHttpForwarderTranslation.gaugerate.host",
			M: &pb.RawMetricV1_Gauge{
				Gauge: &pb.RawGaugeV1{
					Value: 12346,
				},
			},
		},
		{
			Name:     "TestHttpForwarderTranslation.counter",
			Tags:     []string{"TestHttpForwarderTranslation.counter.tag1", "TestHttpForwarderTranslation.counter.tag2"},
			Hostname: "TestHttpForwarderTranslation.counter.host",
			M: &pb.RawMetricV1_Counter{
				Counter: &pb.RawCounterV1{
					Value: 12347,
				},
			},
		},
		{
			Name:     "TestHttpForwarderTranslation.counterrate",
			Tags:     []string{"TestHttpForwarderTranslation.counterrate.tag1", "TestHttpForwarderTranslation.counterrate.tag2"},
			Hostname: "TestHttpForwarderTranslation.counterrate.host",
			M: &pb.RawMetricV1_Counter{
				Counter: &pb.RawCounterV1{
					Value: 123480, // multiplied out
				},
			},
		},
		{
			Name:     "TestHttpForwarderTranslation.timer",
			Tags:     []string{"TestHttpForwarderTranslation.timer.tag1", "TestHttpForwarderTranslation.timer.tag2"},
			Hostname: "TestHttpForwarderTranslation.timer.host",
			M: &pb.RawMetricV1_Timer{
				Timer: &pb.RawTimerV1{
					Value: 12349,
					Rate:  1,
				},
			},
		},
		{
			Name:     "TestHttpForwarderTranslation.timerrate",
			Tags:     []string{"TestHttpForwarderTranslation.timerrate.tag1", "TestHttpForwarderTranslation.timerrate.tag2"},
			Hostname: "TestHttpForwarderTranslation.timerrate.host",
			M: &pb.RawMetricV1_Timer{
				Timer: &pb.RawTimerV1{
					Value: 12350,
					Rate:  0.1, // propagated
				},
			},
		},
		{
			Name:     "TestHttpForwarderTranslation.set",
			Tags:     []string{"TestHttpForwarderTranslation.set.tag1", "TestHttpForwarderTranslation.set.tag2"},
			Hostname: "TestHttpForwarderTranslation.set.host",
			M: &pb.RawMetricV1_Set{
				Set: &pb.RawSetV1{
					Value: "12351",
				},
			},
		},
		{
			Name:     "TestHttpForwarderTranslation.setrate",
			Tags:     []string{"TestHttpForwarderTranslation.setrate.tag1", "TestHttpForwarderTranslation.setrate.tag2"},
			Hostname: "TestHttpForwarderTranslation.setrate.host",
			M: &pb.RawMetricV1_Set{
				Set: &pb.RawSetV1{
					Value: "12352",
				},
			},
		},
	}
	require.EqualValues(t, pbMetrics, expected)
}

func BenchmarkHttpForwarderTranslateAll(b *testing.B) {
	metrics := []*gostatsd.Metric{}

	for i := 0; i < b.N; i++ {
		metrics = append(metrics, &gostatsd.Metric{
			Name:        "bench.metric",
			Value:       123.456,
			StringValue: "123.456",
			Tags:        gostatsd.Tags{"tag1", "tag2"},
			Hostname:    "hostname",
			SourceIP:    "sourceip",
			Type:        1 + gostatsd.MetricType(i%4), // Use all types
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	translateToProtobuf(metrics)
}
