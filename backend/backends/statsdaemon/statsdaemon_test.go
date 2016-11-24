package statsdaemon

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var longName = strings.Repeat("t", maxUDPPacketSize-5)
var m = gostatsd.MetricMap{
	MetricStats: gostatsd.MetricStats{
		NumStats: 1,
	},
	Counters: gostatsd.Counters{
		longName: map[string]gostatsd.Counter{
			"tag1": gostatsd.NewCounter(gostatsd.Nanotime(time.Now().UnixNano()), 5, "", nil),
		},
	},
}

func TestProcessMetricsRecover(t *testing.T) {
	b, err := NewClient("localhost:8125", 1*time.Second, 1*time.Second, false, false)
	require.NoError(t, err)
	c := b.(*client)
	c.processMetrics(&m, func(buf *bytes.Buffer) (*bytes.Buffer, bool) {
		return nil, true
	})
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
	c.processMetrics(&m, func(buf *bytes.Buffer) (*bytes.Buffer, bool) {
		panic(expectedErr)
	})
	t.Errorf("unreachable %v", err)
}

var gaugeMetic = gostatsd.MetricMap{
	MetricStats: gostatsd.MetricStats{
		NumStats: 1,
	},
	Gauges: gostatsd.Gauges{
		"statsd.processing_time": map[string]gostatsd.Gauge{
			"tag1": gostatsd.NewGauge(gostatsd.Nanotime(time.Now().UnixNano()), 2, "", nil),
		},
	},
}

func TestProcessMetrics(t *testing.T) {
	input := []struct {
		disableTags   bool
		expectedValue string
	}{
		{
			disableTags:   false,
			expectedValue: "statsd.processing_time:2.000000|g|#tag1\n",
		},
		{
			disableTags:   true,
			expectedValue: "statsd.processing_time:2.000000|g\n",
		},
	}
	for _, val := range input {
		t.Run(fmt.Sprintf("disableTags: %t", val.disableTags), func(t *testing.T) {
			b, err := NewClient("localhost:8125", 1*time.Second, 1*time.Second, val.disableTags, false)
			require.NoError(t, err)
			c := b.(*client)
			c.processMetrics(&gaugeMetic, func(buf *bytes.Buffer) (*bytes.Buffer, bool) {
				assert.EqualValues(t, val.expectedValue, buf.String())
				return new(bytes.Buffer), false
			})
		})
	}
}
