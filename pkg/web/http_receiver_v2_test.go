package web_test

import (
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/ash2k/stager/wait"
	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/statsd"
	"github.com/atlassian/gostatsd/pkg/web"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/tilinna/clock"
)

func TestForwardingEndToEndV2(t *testing.T) {
	t.Parallel()

	ctxTest, testDone := testContext(t)
	mockClock := clock.NewMock(time.Unix(0, 0))
	ctxTest = clock.Context(ctxTest, mockClock)

	ch := &capturingHandler{}

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

	hfh, err := statsd.NewHttpForwarderHandlerV2(
		logrus.StandardLogger(),
		c.URL,
		"tcp4",
		5, // deliberately prime, so the loop below doesn't send the same thing to the same MetricMap every time.
		10,
		false,
		false,
		10*time.Second,
		10*time.Second,
		10*time.Millisecond,
	)
	require.NoError(t, err)

	var wg wait.Group
	wg.StartWithContext(ctxTest, hfh.Run)
	defer wg.Wait()

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

	for i := 0; i < 100; i++ {
		hfh.DispatchMetrics(ctxTest, []*gostatsd.Metric{m1, m2, m5, m6, m7, m8})
	}
	// only do timers once, because they're very noisy in the output.
	hfh.DispatchMetrics(ctxTest, []*gostatsd.Metric{m3, m4})

	// There's no good way to tell when the Ticker has been created, so we use a hard loop
	for _, d := mockClock.AddNext(); d == 0 && ctxTest.Err() == nil; _, d = mockClock.AddNext() {
		time.Sleep(time.Millisecond) // Allows the system to actually idle, runtime.Gosched() does not.
	}
	mockClock.Add(1 * time.Second)    // Make sure everything gets scheduled
	time.Sleep(50 * time.Millisecond) // Give the http call time to actually occur

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

	actual := ch.GetMetrics()
	for _, metric := range actual {
		metric.Timestamp = 0 // This isn't propagated through v2, and is set to the time of receive
	}

	cmpSort := func(slice []*gostatsd.Metric) func(i, j int) bool {
		return func(i, j int) bool {
			if slice[i].Name == slice[j].Name {
				if len(slice[i].Tags) == len(slice[j].Tags) { // This is not exactly accurate, but close enough with our data
					if slice[i].Type == gostatsd.SET {
						return slice[i].StringValue < slice[j].StringValue
					} else {
						return slice[i].Value < slice[j].Value
					}
				}
				return len(slice[i].Tags) < len(slice[j].Tags)
			}
			return slice[i].Name < slice[j].Name
		}
	}

	sort.Slice(actual, cmpSort(actual))
	sort.Slice(expected, cmpSort(expected))

	require.EqualValues(t, expected, actual)
	testDone()
}
