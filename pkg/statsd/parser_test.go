package statsd

import (
	"context"
	"strconv"
	"testing"

	"github.com/atlassian/gostatsd"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

type metricAndEvent struct {
	metrics []gostatsd.Metric
	events  gostatsd.Events
}

const fakeIP = gostatsd.IP("127.0.0.1")

func newTestParser(ignoreHost bool) (*DatagramParser, *countingHandler) {
	ch := &countingHandler{}
	return NewDatagramParser(nil, "", ignoreHost, 0, ch, rate.Limit(0)), ch
}

func TestParseEmptyDatagram(t *testing.T) {
	t.Parallel()
	input := [][]byte{
		{},
		{'\n'},
		{'\n', '\n'},
	}
	for pos, inp := range input {
		inp := inp
		t.Run(strconv.Itoa(pos), func(t *testing.T) {
			t.Parallel()
			mr, ch := newTestParser(false)
			_, _, _ = mr.handleDatagram(context.Background(), 0, gostatsd.UnknownIP, inp)
			assert.Zero(t, len(ch.events), ch.events)
			assert.Zero(t, len(ch.metrics), ch.metrics)
		})
	}
}

func TestParseDatagram(t *testing.T) {
	t.Parallel()
	input := map[string]metricAndEvent{
		"f:2|c": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"f:2|c\n": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"f:2|c|#t": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1, Tags: gostatsd.Tags{"t"}},
			},
		},
		"f:2|c|#host:h": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1, Tags: gostatsd.Tags{"host:h"}},
			},
		},
		"f:2|c\nx:3|c": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1},
				{Name: "x", Value: 3, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"f:2|c\nx:3|c\n": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1},
				{Name: "x", Value: 3, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"_e{1,1}:a|b\nf:6|c": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 6, SourceIP: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1},
			},
			events: gostatsd.Events{
				gostatsd.Event{Title: "a", Text: "b", SourceIP: "127.0.0.1"},
			},
		},
	}
	for datagram, mAndE := range input {
		datagram := datagram
		mAndE := mAndE
		t.Run(datagram, func(t *testing.T) {
			t.Parallel()
			mr, ch := newTestParser(false)
			_, _, _ = mr.handleDatagram(context.Background(), 0, fakeIP, []byte(datagram))
			for i, e := range ch.events {
				if e.DateHappened <= 0 {
					t.Errorf("%q: DateHappened should be positive", e)
				}
				ch.events[i].DateHappened = 0
			}
			assert.Equal(t, mAndE.events, ch.events)
			assert.Equal(t, mAndE.metrics, ch.metrics)
		})
	}
}

func TestParseDatagramIgnoreHost(t *testing.T) {
	t.Parallel()
	input := map[string]metricAndEvent{
		"f:2|c": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"f:2|c\n": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"f:2|c|#t": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, Type: gostatsd.COUNTER, Rate: 1, Tags: gostatsd.Tags{"t"}},
			},
		},
		"f:2|c|#host:h": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, Hostname: "h", Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"f:2|c|#host:h1,host:h2": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, Hostname: "h1", Type: gostatsd.COUNTER, Rate: 1, Tags: gostatsd.Tags{"host:h2"}},
			},
		},
		"f:2|c\nx:3|c": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, Type: gostatsd.COUNTER, Rate: 1},
				{Name: "x", Value: 3, Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"f:2|c\nx:3|c\n": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 2, Type: gostatsd.COUNTER, Rate: 1},
				{Name: "x", Value: 3, Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"_e{1,1}:a|b\nf:6|c": {
			metrics: []gostatsd.Metric{
				{Name: "f", Value: 6, Type: gostatsd.COUNTER, Rate: 1},
			},
			events: gostatsd.Events{
				gostatsd.Event{Title: "a", Text: "b", SourceIP: "127.0.0.1"},
			},
		},
	}
	for datagram, mAndE := range input {
		datagram := datagram
		mAndE := mAndE
		t.Run(datagram, func(t *testing.T) {
			t.Parallel()
			mr, ch := newTestParser(true)
			_, _, _ = mr.handleDatagram(context.Background(), 0, fakeIP, []byte(datagram))
			for i, e := range ch.events {
				if e.DateHappened <= 0 {
					t.Errorf("%q: DateHappened should be positive", e)
				}
				ch.events[i].DateHappened = 0
			}
			assert.Equal(t, mAndE.events, ch.events)
			assert.Equal(t, mAndE.metrics, ch.metrics)
		})
	}
}
