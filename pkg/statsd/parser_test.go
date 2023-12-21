package statsd

import (
	"context"
	"sort"
	"strconv"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/fixtures"
	"github.com/atlassian/gostatsd/internal/lexer"
	"github.com/atlassian/gostatsd/internal/pool"
)

type metricAndEvent struct {
	metrics []*gostatsd.Metric
	events  gostatsd.Events
}

const fakeIP = gostatsd.Source("127.0.0.1")

func lex() *lexer.Lexer {
	return &lexer.Lexer{
		MetricPool: pool.NewMetricPool(0),
	}
}

func newTestParser(ignoreHost bool) (*DatagramParser, *countingHandler) {
	ch := &countingHandler{}
	return NewDatagramParser(nil, "", ignoreHost, 0, ch, rate.Limit(0), false, logrus.New()), ch
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
			_, _, _ = mr.handleDatagram(context.Background(), lex(), 0, gostatsd.UnknownSource, inp)
			assert.Zero(t, len(ch.events), ch.events)
			assert.Zero(t, len(ch.metrics), ch.metrics)
		})
	}
}

func TestParseDatagram(t *testing.T) {
	t.Parallel()
	input := map[string]metricAndEvent{
		"f:2|c": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 2, Source: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"f:2|c\n": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 2, Source: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"f:2|c|#t": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 2, Source: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1, Tags: gostatsd.Tags{"t"}},
			},
		},
		"f:2|c|#host:h": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 2, Source: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1, Tags: gostatsd.Tags{"host:h"}},
			},
		},
		"f:2|c\nx:3|c": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 2, Source: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1},
				{Name: "x", Value: 3, Source: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"f:2|c\nx:3|c\n": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 2, Source: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1},
				{Name: "x", Value: 3, Source: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"_e{1,1}:a|b\nf:6|c": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 6, Source: "127.0.0.1", Type: gostatsd.COUNTER, Rate: 1},
			},
			events: gostatsd.Events{
				{Title: "a", Text: "b", Source: "127.0.0.1"},
			},
		},
	}
	for datagram, mAndE := range input {
		datagram := datagram
		mAndE := mAndE
		t.Run(datagram, func(t *testing.T) {
			t.Parallel()
			mr, ch := newTestParser(false)
			metrics, _, _ := mr.handleDatagram(context.Background(), lex(), 0, fakeIP, []byte(datagram))
			mm := gostatsd.NewMetricMap(false)
			for _, m := range metrics {
				mm.Receive(m)
			}
			for i, e := range ch.events {
				if e.DateHappened <= 0 {
					t.Errorf("%q: DateHappened should be positive", e)
				}
				ch.events[i].DateHappened = 0
			}

			ch.DispatchMetricMap(context.Background(), mm)
			actual := gostatsd.MergeMaps(ch.MetricMaps()).AsMetrics()
			for _, m := range mAndE.metrics {
				m.FormatTagsKey()
			}
			sort.Slice(actual, fixtures.SortCompare(actual))
			sort.Slice(mAndE.metrics, fixtures.SortCompare(mAndE.metrics))

			assert.Equal(t, mAndE.events, ch.events)
			assert.Equal(t, mAndE.metrics, actual)
		})
	}
}

func TestParseDatagramIgnoreHost(t *testing.T) {
	t.Parallel()
	input := map[string]metricAndEvent{
		"f:2|c": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 2, Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"f:2|c\n": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 2, Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"f:2|c|#t": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 2, Type: gostatsd.COUNTER, Rate: 1, Tags: gostatsd.Tags{"t"}},
			},
		},
		"f:2|c|#host:h": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 2, Source: "h", Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"f:2|c|#host:h1,host:h2": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 2, Source: "h1", Type: gostatsd.COUNTER, Rate: 1, Tags: gostatsd.Tags{"host:h2"}},
			},
		},
		"f:2|c\nx:3|c": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 2, Type: gostatsd.COUNTER, Rate: 1},
				{Name: "x", Value: 3, Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"f:2|c\nx:3|c\n": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 2, Type: gostatsd.COUNTER, Rate: 1},
				{Name: "x", Value: 3, Type: gostatsd.COUNTER, Rate: 1},
			},
		},
		"_e{1,1}:a|b\nf:6|c": {
			metrics: []*gostatsd.Metric{
				{Name: "f", Value: 6, Type: gostatsd.COUNTER, Rate: 1},
			},
			events: gostatsd.Events{
				{Title: "a", Text: "b", Source: "127.0.0.1"},
			},
		},
	}
	for datagram, mAndE := range input {
		datagram := datagram
		mAndE := mAndE
		t.Run(datagram, func(t *testing.T) {
			t.Parallel()
			mr, ch := newTestParser(true)
			metrics, _, _ := mr.handleDatagram(context.Background(), lex(), 0, fakeIP, []byte(datagram))
			for i, e := range ch.events {
				if e.DateHappened <= 0 {
					t.Errorf("%q: DateHappened should be positive", e)
				}
				ch.events[i].DateHappened = 0
			}
			mm := gostatsd.NewMetricMap(false)
			for _, m := range metrics {
				mm.Receive(m)
			}
			ch.DispatchMetricMap(context.Background(), mm)
			actual := gostatsd.MergeMaps(ch.MetricMaps()).AsMetrics()
			for _, m := range mAndE.metrics {
				m.FormatTagsKey()
			}

			sort.Slice(actual, fixtures.SortCompare(actual))
			sort.Slice(mAndE.metrics, fixtures.SortCompare(mAndE.metrics))

			assert.Equal(t, mAndE.events, ch.events)
			assert.Equal(t, mAndE.metrics, actual)
		})
	}
}
