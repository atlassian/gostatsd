package statsdaemon

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/atlassian/gostatsd/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var longName = strings.Repeat("t", maxUDPPacketSize-5)
var m = types.MetricMap{
	MetricStats: types.MetricStats{
		NumStats: 1,
	},
	Counters: types.Counters{
		longName: map[string]types.Counter{
			"tag1": types.NewCounter(types.Nanotime(time.Now().UnixNano()), 5, "", nil),
		},
	},
}

func TestProcessMetricsRecover(t *testing.T) {
	b, err := NewClient("localhost:8125", 1*time.Second, 1*time.Second, false, false)
	require.NoError(t, err)
	c := b.(*client)
	expectedErr := errors.New("ABC some error")
	actualErr := c.processMetrics(&m, false, func(buf *bytes.Buffer) (*bytes.Buffer, error) {
		return buf, expectedErr
	})
	assert.Equal(t, expectedErr, actualErr)
}

func TestProcessMetricsPanic(t *testing.T) {
	b, err := NewClient("localhost:8125", 1*time.Second, 1*time.Second, false, false)
	require.NoError(t, err)
	c := b.(*client)
	expectedErr := errors.New("ABC some error")
	defer func() {
		if r := recover(); r != nil {
			assert.Equal(t, expectedErr, r)
		} else {
			t.Error("should have panicked")
		}
	}()
	err = c.processMetrics(&m, false, func(buf *bytes.Buffer) (*bytes.Buffer, error) {
		panic(expectedErr)
	})
	t.Errorf("unreachable %v", err)
}

var gaugeMetic = types.MetricMap{
	MetricStats: types.MetricStats{
		NumStats: 1,
	},
	Gauges: types.Gauges{
		"statsd.processing_time": map[string]types.Gauge{
			"tag1": types.NewGauge(types.Nanotime(time.Now().UnixNano()), 2, "", nil),
		},
	},
}

func TestProcessMetricsTags(t *testing.T) {
	b, err := NewClient("localhost:8125", 1*time.Second, 1*time.Second, false, false)
	require.NoError(t, err)
	c := b.(*client)
	expectedValue := "statsd.processing_time:2.000000|g|#tag1\n"
	assert.NoError(t, c.processMetrics(&gaugeMetic, false, func(buf *bytes.Buffer) (*bytes.Buffer, error) {
		assert.EqualValues(t, expectedValue, buf.String())
		return new(bytes.Buffer), nil
	}))

	expectedValue = "statsd.processing_time:2.000000|g\n"
	assert.NoError(t, c.processMetrics(&gaugeMetic, true, func(buf *bytes.Buffer) (*bytes.Buffer, error) {
		assert.EqualValues(t, expectedValue, buf.String())
		return new(bytes.Buffer), nil
	}))
}
