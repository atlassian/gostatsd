package stats

import (
	"context"

	"github.com/atlassian/gostatsd"
)

// ReportFunc allows for configurable metric types for internal metrics
type ReportFunc func(name string, val float64, tags gostatsd.Tags)

type reportTypeKey int

const reportTypeContextKey = reportTypeKey(0)

func (rf ReportFunc) Report(name string, val float64, tags gostatsd.Tags) {
	if rf == nil {
		return
	}
	rf(name, val, tags)
}

// NewReportFunc returns a method that will produce
// either Counter, Gauge, or Timing. All other values are ignored to allow
// disabling metrics
func NewReportFunc(stats Statser, mType gostatsd.MetricType) ReportFunc {
	switch mType {
	case gostatsd.COUNTER:
		return stats.Count
	case gostatsd.GAUGE:
		return stats.Gauge
	case gostatsd.TIMER:
		return stats.TimingMS
	}
	return nil
}

func NewReportContext(ctx context.Context, mType gostatsd.MetricType) context.Context {
	return context.WithValue(ctx, reportTypeContextKey, mType)
}

func ReportFromContext(ctx context.Context, statser Statser) ReportFunc {
	if mType, ok := ctx.Value(reportTypeContextKey).(gostatsd.MetricType); ok {
		return NewReportFunc(statser, mType)
	}
	return nil
}
