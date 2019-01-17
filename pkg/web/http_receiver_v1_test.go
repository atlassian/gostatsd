package web_test

import (
	"net/http/httptest"
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

func TestForwardingEndToEndV1(t *testing.T) {
	t.Parallel()

	ctxTest, testDone := testContext(t)
	mockClock := clock.NewMock(time.Unix(0, 0))
	ctxTest = clock.Context(ctxTest, mockClock)

	ch := &capturingHandler{}

	hs, err := web.NewHttpServer(
		logrus.StandardLogger(),
		ch,
		"TestForwardingEndToEndV1",
		"",
		false,
		false,
		true,
		false,
	)
	require.NoError(t, err)

	c := httptest.NewServer(hs.Router)
	defer c.Close()

	hfh, err := statsd.NewHttpForwarderHandlerV1(
		logrus.StandardLogger(),
		c.URL,
		"tcp4",
		100,
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
		Name:  "foo",
		Type:  gostatsd.COUNTER,
		Value: 10,
		Rate:  1,
	}
	m2before := &gostatsd.Metric{
		Name:  "foo",
		Type:  gostatsd.COUNTER,
		Value: 10,
		Rate:  0.1,
	}
	hfh.DispatchMetric(ctxTest, m1)
	hfh.DispatchMetric(ctxTest, m2before)

	// There's no good way to tell when the Ticker has been created, so we use a hard loop
	for _, d := mockClock.AddNext(); d == 0 && ctxTest.Err() == nil; _, d = mockClock.AddNext() {
		time.Sleep(time.Millisecond) // Allows the system to actually idle, runtime.Gosched() does not.
	}
	mockClock.Add(1 * time.Second)    // Make sure everything gets scheduled
	time.Sleep(50 * time.Millisecond) // Give the http call time to actually occur

	m2after := &gostatsd.Metric{
		Name:  "foo",
		Type:  gostatsd.COUNTER,
		Value: 100, // Rate is multiplied out
		Rate:  1,
	}
	expected := []*gostatsd.Metric{m1, m2after}
	actual := ch.GetMetrics()
	require.EqualValues(t, expected, actual)
	testDone()
}
