package statsd

import (
	"context"
	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/ready"
	"github.com/tilinna/clock"
	"sync"
	"testing"
	"time"

	"github.com/atlassian/gostatsd/pkg/cluster/nodes"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func testContext(t *testing.T) (context.Context, func()) {
	ctxTest, completeTest := context.WithTimeout(context.Background(), 1100*time.Millisecond)
	go func() {
		after := time.NewTimer(1 * time.Second)
		select {
		case <-ctxTest.Done():
			after.Stop()
		case <-after.C:
			require.Fail(t, "test timed out")
		}
	}()
	return ctxTest, completeTest
}

func TestClusterHandlerLocalOnly(t *testing.T) {
	ctxTest, cancel := testContext(t)
	defer cancel()

	local := newCapturingHandler(1)
	remote := NewCapturingClusterForwarder(0)
	ch, err := NewClusterHandler(logrus.StandardLogger(), local, remote, nodes.NewSingleNodePicker(nodes.NodeNameSelf))
	require.NoError(t, err)

	var wgReady sync.WaitGroup
	mockClock := clock.NewMock(time.Unix(0, 0))
	ctxTest = ready.WithWaitGroup(clock.Context(ctxTest, mockClock), &wgReady)
	wgReady.Add(1)
	go ch.Run(ctxTest)
	wgReady.Wait()

	mm := gostatsd.NewMetricMap(false)
	metric := &gostatsd.Metric{
		Type:  gostatsd.COUNTER,
		Name:  "foo",
		Tags:  gostatsd.Tags{},
		Value: 1,
		Rate:  1,
	}
	mm.Receive(metric)
	ch.DispatchMetricMap(ctxTest, mm)

	// Send the metric to a regular map to generate expected data.
	expectedMM := gostatsd.NewMetricMap(false)
	expectedMM.Receive(metric)

	mockClock.Add(150 * time.Millisecond)

	require.True(t, local.wait(ctxTest))
	require.True(t, remote.wait(ctxTest))
	require.EqualValues(t, 1, len(local.mm))
	require.EqualValues(t, expectedMM, local.mm[0])

	require.EqualValues(t, 0, len(remote.Maps()))
}

func TestClusterHandlerRemoteOnly(t *testing.T) {
	ctxTest, cancel := testContext(t)
	defer cancel()

	local := newCapturingHandler(0)
	remote := NewCapturingClusterForwarder(1)
	ch, err := NewClusterHandler(logrus.StandardLogger(), local, remote, nodes.NewSingleNodePicker("1.2.3.4"))
	require.NoError(t, err)

	var wgReady sync.WaitGroup
	mockClock := clock.NewMock(time.Unix(0, 0))
	ctxTest = ready.WithWaitGroup(clock.Context(ctxTest, mockClock), &wgReady)
	wgReady.Add(1)
	go ch.Run(ctxTest)
	wgReady.Wait()

	mm := gostatsd.NewMetricMap(false)
	metric := &gostatsd.Metric{
		Type:  gostatsd.COUNTER,
		Name:  "foo2",
		Tags:  gostatsd.Tags{},
		Value: 1,
		Rate:  1,
	}
	mm.Receive(metric)
	ch.DispatchMetricMap(ctxTest, mm)

	// Send the metric to a regular map to generate expected data.
	expectedMM := gostatsd.NewMetricMap(false)
	expectedMM.Receive(metric)

	mockClock.Add(150 * time.Millisecond)

	require.True(t, local.wait(ctxTest))
	require.EqualValues(t, 0, len(local.mm))

	require.True(t, remote.wait(ctxTest))
	require.EqualValues(t, 1, len(remote.Maps()))
	expected := map[string][]*gostatsd.MetricMap{
		"1.2.3.4": {expectedMM},
	}
	require.EqualValues(t, expected, remote.Maps())
}
