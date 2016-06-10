package statsdaemon

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/atlassian/gostatsd/types"
)

var longName = strings.Repeat("t", maxUDPPacketSize-5)
var m = types.MetricMap{
	NumStats: 1,
	Counters: types.Counters{
		longName: map[string]types.Counter{
			"tag1": types.NewCounter(time.Now(), 1*time.Second, 5),
		},
	},
}

func TestProcessMetricsRecover(t *testing.T) {
	expectedErr := errors.New("ABC some error")
	actualErr := processMetrics(&m, func(buf *bytes.Buffer) (*bytes.Buffer, error) {
		return nil, expectedErr
	})
	if expectedErr != actualErr {
		t.Errorf("expected %v, actual %v", expectedErr, actualErr)
	}
}

func TestProcessMetricsPanic(t *testing.T) {
	expectedErr := errors.New("ABC some error")
	defer func() {
		if r := recover(); r != nil {
			if r != expectedErr {
				t.Errorf("expected %v, got %v", expectedErr, r)
			}
		} else {
			t.Error("should have panicked")
		}
	}()
	err := processMetrics(&m, func(buf *bytes.Buffer) (*bytes.Buffer, error) {
		panic(expectedErr)
	})
	t.Errorf("unreachable %v", err)
}
