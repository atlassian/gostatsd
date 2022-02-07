package web_test

import (
	"context"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/ash2k/stager/wait"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tilinna/clock"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/fixtures"
	"github.com/atlassian/gostatsd/pkg/statsd"
	"github.com/atlassian/gostatsd/pkg/transport"
	"github.com/atlassian/gostatsd/pkg/web"
)

func TestForwardingEndToEndV2(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	mockClock := clock.NewMock(time.Unix(0, 0))
	ctx = clock.Context(ctx, mockClock)

	ch := &channeledHandler{
		chMaps: make(chan *gostatsd.MetricMap),
	}

	hs, err := web.NewHttpServer(
		logrus.StandardLogger(),
		ch,
		"TestForwardingEndToEndV2",
		"",
		false,
		false,
		true,
		false,
	)
	require.NoError(t, err)

	c := httptest.NewServer(hs.Router)
	defer c.Close()

	p := transport.NewTransportPool(logrus.New(), viper.New())
	hfh, err := statsd.NewHttpForwarderHandlerV2(
		logrus.StandardLogger(),
		"default",
		c.URL,
		5, // deliberately prime, so the loop below doesn't send the same thing to the same MetricMap every time.
		10,
		false,
		10*time.Second,
		10*time.Millisecond,
		nil,
		nil,
		p,
	)
	require.NoError(t, err)

	var wg wait.Group
	wg.StartWithContext(ctx, hfh.Run)
	defer wg.Wait()
	defer cancel() // cancel must occur before waiting for the wg

	m1 := &gostatsd.Metric{
		Name:  "counter",
		Type:  gostatsd.COUNTER,
		Value: 10,
		Rate:  1,
	}
	m2 := &gostatsd.Metric{
		Name:  "counter",
		Type:  gostatsd.COUNTER,
		Value: 10,
		Rate:  0.1,
	}
	m3 := &gostatsd.Metric{
		Name:  "timer",
		Type:  gostatsd.TIMER,
		Value: 10,
		Rate:  1,
	}
	m4 := &gostatsd.Metric{
		Name:  "timer",
		Type:  gostatsd.TIMER,
		Value: 10,
		Rate:  0.1,
	}
	m5 := &gostatsd.Metric{
		Name:  "gauge",
		Type:  gostatsd.GAUGE,
		Value: 10,
		Rate:  1,
	}
	m6 := &gostatsd.Metric{
		Name:  "gauge",
		Type:  gostatsd.GAUGE,
		Value: 10,
		Rate:  0.1,
	}
	m7 := &gostatsd.Metric{
		Name:        "set",
		Type:        gostatsd.SET,
		StringValue: "abc",
		Rate:        1,
	}
	m8 := &gostatsd.Metric{
		Name:        "set",
		Type:        gostatsd.SET,
		StringValue: "def",
		Rate:        0.1,
	}

	mm := gostatsd.NewMetricMap(false)

	for i := 0; i < 100; i++ {
		mm.Receive(m1)
		mm.Receive(m2)
		mm.Receive(m5)
		mm.Receive(m6)
		mm.Receive(m7)
		mm.Receive(m8)
	}
	// only do timers once, because they're very noisy in the output.
	mm.Receive(m3)
	mm.Receive(m4)
	hfh.DispatchMetricMap(ctx, mm)

	fixtures.NextStep(ctx, mockClock)
	mockClock.Add(1 * time.Second) // Make sure everything gets scheduled

	expected := []*gostatsd.Metric{
		{Name: "counter", Type: gostatsd.COUNTER, Value: (100 * 10) + (100 * 10 / 0.1), Rate: 1},
		{Name: "gauge", Type: gostatsd.GAUGE, Value: 10, Rate: 1},
		// 10 = the sample count for the timer where rate=0.1
		// 1 = the sample count for the timer where rate=1
		// 2 = number of timers
		{Name: "timer", Type: gostatsd.TIMER, Value: 10, Rate: 1.0 / ((10.0 + 1.0) / 2.0)},
		{Name: "timer", Type: gostatsd.TIMER, Value: 10, Rate: 1.0 / ((10.0 + 1.0) / 2.0)},
		{Name: "set", Type: gostatsd.SET, StringValue: "abc", Rate: 1},
		{Name: "set", Type: gostatsd.SET, StringValue: "def", Rate: 1},
	}
	for _, m := range expected {
		m.FormatTagsKey()
	}

	var maps []*gostatsd.MetricMap
	var actual []*gostatsd.Metric
	for !assert.ObjectsAreEqualValues(expected, actual) {
		// Receive another map
		select {
		case <-ctx.Done():
			t.FailNow()
		case mm := <-ch.chMaps:
			maps = append(maps, mm)
		}

		// Merge in to a Metric slice
		actual = gostatsd.MergeMaps(maps).AsMetrics()

		// Normalize
		for _, metric := range actual {
			metric.Timestamp = 0 // This isn't propagated through v2, and is set to the time of receive
		}
		sort.Slice(actual, fixtures.SortCompare(actual))
		sort.Slice(expected, fixtures.SortCompare(expected))

		// Test on next loop iteration
	}
}
