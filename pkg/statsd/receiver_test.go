package statsd

import (
	"context"
	"reflect"
	"testing"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/fakesocket"
)

type metricAndEvent struct {
	metrics []gostatsd.Metric
	events  gostatsd.Events
}

var receiveBlackhole error

func TestReceiveEmptyPacket(t *testing.T) {
	input := [][]byte{
		{},
		{'\n'},
		{'\n', '\n'},
	}
	for _, inp := range input {
		ch := &countingHandler{}
		mr := NewMetricReceiver("", ch)

		err := mr.handlePacket(context.Background(), fakesocket.FakeAddr, inp)
		if err != nil {
			t.Errorf("%q: unexpected error: %v", inp, err)
		}
		if len(ch.events) > 0 {
			t.Errorf("%q: expected no events: %v", inp, ch.events)
		}
		if len(ch.metrics) > 0 {
			t.Errorf("%q: expected no metrics: %v", inp, ch.metrics)
		}
	}
}

func TestReceivePacket(t *testing.T) {
	input := map[string]metricAndEvent{
		"f:2|c": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER},
			},
		},
		"f:2|c\n": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER},
			},
		},
		"f:2|c\nx:3|c": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER},
				{Name: "x", Value: 3, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER},
			},
		},
		"f:2|c\nx:3|c\n": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER},
				{Name: "x", Value: 3, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER},
			},
		},
		"_e{1,1}:a|b\nf:6|c": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 6, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER},
			},
			events: gostatsd.Events{
				gostatsd.Event{Title: "a", Text: "b", SourceIP: "127.0.0.1"},
			},
		},
	}
	for packet, mAndE := range input {
		ch := &countingHandler{}
		mr := NewMetricReceiver("", ch)

		err := mr.handlePacket(context.Background(), fakesocket.FakeAddr, []byte(packet))
		if err != nil {
			t.Errorf("%q: unexpected error: %v", packet, err)
		}
		for i, e := range ch.events {
			if e.DateHappened <= 0 {
				t.Errorf("%q: DateHappened should be positive", e)
			}
			ch.events[i].DateHappened = 0
		}
		if !reflect.DeepEqual(ch.events, mAndE.events) {
			t.Errorf("%q: expected to be equal:\n%v\n%v", packet, ch.events, mAndE.events)
		}
		if !reflect.DeepEqual(ch.metrics, mAndE.metrics) {
			t.Errorf("%q: expected to be equal:\n%v\n%v", packet, ch.metrics, mAndE.metrics)
		}
	}
}

func BenchmarkReceive(b *testing.B) {
	mr := &MetricReceiver{
		handler: nopHandler{},
	}
	c := fakesocket.FakePacketConn{}
	ctx := context.Background()
	var r error
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		r = mr.Receive(ctx, c)
	}
	receiveBlackhole = r
}

type nopHandler struct{}

func (h nopHandler) DispatchMetric(ctx context.Context, m *gostatsd.Metric) error {
	return context.Canceled // Stops receiver after first read is done
}

func (h nopHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) error {
	return context.Canceled // Stops receiver after first read is done
}

func (h nopHandler) WaitForEvents() {
}
