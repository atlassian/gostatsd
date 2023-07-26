package stats

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/atlassian/gostatsd"
)

type MockStatser struct {
	mock.Mock
}

func (ms *MockStatser) NotifyFlush(context.Context, time.Duration) {

}

func (ms *MockStatser) RegisterFlush() (<-chan time.Duration, func()) {
	return nil, func() {}
}

func (ms *MockStatser) WaitForEvents() {

}

func (ms *MockStatser) Event(context.Context, *gostatsd.Event) {

}

func (ms *MockStatser) WithTags(gostatsd.Tags) Statser {
	return &NullStatser{}
}

func (ms *MockStatser) NewTimer(name string, tags gostatsd.Tags) *Timer {
	return newTimer(ms, name, tags)
}

func (ms *MockStatser) TimingDuration(name string, d time.Duration, tags gostatsd.Tags) {

}

func (ms *MockStatser) TimingMS(name string, val float64, tags gostatsd.Tags) {
	ms.Called(name, val, tags)
}

func (ms *MockStatser) Increment(name string, tags gostatsd.Tags) {

}

func (ms *MockStatser) Count(name string, val float64, tags gostatsd.Tags) {
	ms.Called(name, val, tags)
}

func (ms *MockStatser) Gauge(name string, val float64, tags gostatsd.Tags) {
	ms.Called(name, val, tags)
}

func TestReport(t *testing.T) {
	t.Parallel()

	var (
		name = "metric.name"
		val  = float64(1.0)
		tags = gostatsd.Tags([]string{})
	)

	for _, tc := range []struct {
		name    string
		ctx     context.Context
		statser *MockStatser
	}{
		{
			name:    "Empty context",
			ctx:     context.Background(),
			statser: &MockStatser{},
		},
		{
			name:    "Unknown type",
			ctx:     NewReportContext(context.Background(), gostatsd.MetricType(0)),
			statser: &MockStatser{},
		},
		{
			name: "Gauge reporter",
			ctx:  NewReportContext(context.Background(), gostatsd.GAUGE),
			statser: func() *MockStatser {
				ms := &MockStatser{}
				ms.On("Gauge", name, val, tags)
				return ms
			}(),
		},
		{
			name: "Counter reporter",
			ctx:  NewReportContext(context.Background(), gostatsd.COUNTER),
			statser: func() *MockStatser {
				ms := &MockStatser{}
				ms.On("Count", name, val, tags)
				return ms
			}(),
		},
		{
			name: "Timer reporter",
			ctx:  NewReportContext(context.Background(), gostatsd.TIMER),
			statser: func() *MockStatser {
				ms := &MockStatser{}
				ms.On("TimingMS", name, val, tags)
				return ms
			}(),
		},
	} {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			t.Cleanup(func() {
				tc.statser.AssertExpectations(t)
			})

			reporter := ReportFromContext(tc.ctx, tc.statser)
			reporter.Report(name, val, tags)
		})
	}
}
