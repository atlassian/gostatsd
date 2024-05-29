package otlp

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/multierr"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/backends/otlp/internal/data"
	"github.com/atlassian/gostatsd/pkg/transport"
	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	BackendName = `otlp`
)

// Backend contains additional meta data in order
// to export values as OTLP metrics.
// The zero value is not safe to use.
type Backend struct {
	droppedMetrics uint64
	droppedEvents  uint64

	endpoint              string
	convertTimersToGauges bool
	is                    data.InstrumentationScope
	resourceKeys          gostatsd.Tags

	discarded               gostatsd.TimerSubtypes
	logger                  logrus.FieldLogger
	client                  *http.Client
	metricRequestsBufferSem chan struct{}
	eventRequestsBufferSem  chan struct{}
}

var _ gostatsd.Backend = (*Backend)(nil)

func NewClientFromViper(v *viper.Viper, logger logrus.FieldLogger, pool *transport.TransportPool) (gostatsd.Backend, error) {
	cfg, err := NewConfig(v)
	if err != nil {
		return nil, err
	}

	tc, err := pool.Get(cfg.Transport)
	if err != nil {
		return nil, err
	}

	version := runtime.Version()
	if bi, ok := debug.ReadBuildInfo(); ok {
		version = bi.Main.Version
	}

	return &Backend{
		endpoint:                cfg.Endpoint,
		convertTimersToGauges:   cfg.Conversion == ConversionAsGauge,
		is:                      data.NewInstrumentationScope("gostatsd/aggregation", version),
		resourceKeys:            cfg.ResourceKeys,
		discarded:               cfg.TimerSubtypes,
		client:                  tc.Client,
		logger:                  logger,
		metricRequestsBufferSem: make(chan struct{}, cfg.MaxRequests),
	}, nil
}

func (*Backend) Name() string {
	return BackendName
}

func (b *Backend) SendEvent(ctx context.Context, event *gostatsd.Event) error {
	eventLog := plog.NewLogRecord()
	lr := eventLog

	title := event.Title
	text := event.Text
	dateHappened := event.DateHappened
	sourceTypeName := event.SourceTypeName
	tags := event.Tags
	source := event.Source
	priority := event.Priority
	alertType := event.AlertType

	attrs := lr.Attributes()
	attrs.EnsureCapacity(1 + len(tags))
	for _, dim := range tags {
		if dim == "" {
			continue
		}

		kvs := strings.SplitN(dim, ":", 1)
		if len(kvs) == 2 {
			attrs.PutStr(kvs[0], kvs[1])
		} else {
			attrs.PutStr(kvs[0], "")
		}
	}

	// The EventType field is stored as an attribute.
	eventType := event.EventType
	if eventType == "" {
		eventType = "unknown"
	}
	attrs.PutStr(splunk.SFxEventType, eventType)

	// SignalFx timestamps are in millis so convert to nanos by multiplying
	// by 1 million.
	lr.SetTimestamp(pcommon.Timestamp(event.Timestamp * 1e6))

	if event.Category != nil {
		attrs.PutInt(splunk.SFxEventCategoryKey, int64(*event.Category))
	} else {
		// This gives us an unambiguous way of determining that a log record
		// represents a SignalFx event, even if category is missing from the
		// event.
		attrs.PutEmpty(splunk.SFxEventCategoryKey)
	}

	//if len(event.Properties) > 0 {
	//	propMap := attrs.PutEmptyMap(splunk.SFxEventPropertiesKey)
	//	propMap.EnsureCapacity(len(event.Properties))
	//	for _, prop := range event.Properties {
	//		// No way to tell what value type is without testing each
	//		// individually.
	//		switch {
	//		case prop.Value.StrValue != nil:
	//			propMap.PutStr(prop.Key, prop.Value.GetStrValue())
	//		case prop.Value.IntValue != nil:
	//			propMap.PutInt(prop.Key, prop.Value.GetIntValue())
	//		case prop.Value.DoubleValue != nil:
	//			propMap.PutDouble(prop.Key, prop.Value.GetDoubleValue())
	//		case prop.Value.BoolValue != nil:
	//			propMap.PutBool(prop.Key, prop.Value.GetBoolValue())
	//		default:
	//			// If there is no property value, just insert a null to
	//			// record that the key was present.
	//			propMap.PutEmpty(prop.Key)
	//		}
	//	}
	//}

	req, err := data.NewEventsRequest(ctx, b.endpoint)
	if err != nil {
		atomic.AddUint64(&b.droppedEvents, 1)
		return err
	}

	b.eventRequestsBufferSem <- struct{}{}
	resp, err := b.client.Do(req)
	<-b.eventRequestsBufferSem
	if err != nil {
		atomic.AddUint64(&b.droppedEvents, 1)
		return err
	}

	//TODO: Implement the ProcessEventResponse function
	//err := data.ProcessEventResponse(resp)
	//if err != nil {
	//  atomic.AddUint64(&b.droppedEvents, 1)
	//}

	return err
}

func (bd *Backend) SendMetricsAsync(ctx context.Context, mm *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	group := make(Group)

	mm.Counters.Each(func(name, _ string, cm gostatsd.Counter) {
		resources, attributes := splitTagsByKeys(cm.Tags, bd.resourceKeys)

		m := data.NewMetric(name).SetSum(
			data.NewSum(
				data.NewNumberDataPoint(
					uint64(cm.Timestamp),
					data.WithNumberDataPointMap(attributes),
					data.WithNumberDatapointIntValue(cm.Value),
				),
			),
		)
		group.Insert(bd.is, resources, m)
	})

	mm.Gauges.Each(func(name, _ string, gm gostatsd.Gauge) {
		resources, attributes := splitTagsByKeys(gm.Tags, bd.resourceKeys)

		m := data.NewMetric(name).SetGauge(
			data.NewGauge(
				data.NewNumberDataPoint(
					uint64(gm.Timestamp),
					data.WithNumberDataPointMap(attributes),
					data.WithNumberDataPointDoubleValue(gm.Value),
				),
			),
		)

		group.Insert(bd.is, resources, m)
	})

	mm.Sets.Each(func(name, _ string, sm gostatsd.Set) {
		resources, attributes := splitTagsByKeys(sm.Tags, bd.resourceKeys)

		m := data.NewMetric(name).SetGauge(
			data.NewGauge(
				data.NewNumberDataPoint(
					uint64(sm.Timestamp),
					data.WithNumberDataPointMap(attributes),
					data.WithNumberDatapointIntValue(int64(len(sm.Values))),
				),
			),
		)

		group.Insert(bd.is, resources, m)
	})

	mm.Timers.Each(func(name, _ string, t gostatsd.Timer) {
		resources, attributes := splitTagsByKeys(t.Tags, bd.resourceKeys)

		switch bd.convertTimersToGauges {
		case true:
			if len(t.Histogram) != 0 {
				btags := data.NewMap()
				btags.Merge(attributes)
				for boundry, count := range t.Histogram {
					if math.IsInf(float64(boundry), 1) {
						btags.Insert("le", "+Inf")
					} else {
						btags.Insert("le", strconv.FormatFloat(float64(boundry), 'f', -1, 64))
					}
					group.Insert(
						bd.is,
						resources,
						data.NewMetric(fmt.Sprintf("%s.histogram", name)).SetGauge(
							data.NewGauge(data.NewNumberDataPoint(
								uint64(t.Timestamp),
								data.WithNumberDataPointMap(btags),
								data.WithNumberDatapointIntValue(int64(count)),
							)),
						),
					)
				}
				return
			}
			calcs := []struct {
				discarded bool
				suffix    string
				value     func(data.NumberDataPoint)
			}{
				{discarded: bd.discarded.Lower, suffix: "lower", value: data.WithNumberDataPointDoubleValue(t.Min)},
				{discarded: bd.discarded.Upper, suffix: "upper", value: data.WithNumberDataPointDoubleValue(t.Max)},
				{discarded: bd.discarded.Count, suffix: "count", value: data.WithNumberDatapointIntValue(int64(t.Count))},
				{discarded: bd.discarded.CountPerSecond, suffix: "count_ps", value: data.WithNumberDataPointDoubleValue(t.PerSecond)},
				{discarded: bd.discarded.Mean, suffix: "mean", value: data.WithNumberDataPointDoubleValue(t.Mean)},
				{discarded: bd.discarded.Median, suffix: "median", value: data.WithNumberDataPointDoubleValue(t.Median)},
				{discarded: bd.discarded.StdDev, suffix: "std", value: data.WithNumberDataPointDoubleValue(t.StdDev)},
				{discarded: bd.discarded.Sum, suffix: "sum", value: data.WithNumberDataPointDoubleValue(t.Sum)},
				{discarded: bd.discarded.SumSquares, suffix: "sum_squares", value: data.WithNumberDataPointDoubleValue(t.SumSquares)},
			}

			for _, calc := range calcs {
				if calc.discarded {
					continue
				}
				group.Insert(
					bd.is,
					resources,
					data.NewMetric(fmt.Sprintf("%s.%s", name, calc.suffix)).SetGauge(
						data.NewGauge(data.NewNumberDataPoint(
							uint64(t.Timestamp),
							data.WithNumberDataPointMap(attributes),
							calc.value,
						)),
					),
				)
			}

			for _, pct := range t.Percentiles {
				group.Insert(bd.is, resources, data.NewMetric(fmt.Sprintf("%s.%s", name, pct.Str)).SetGauge(
					data.NewGauge(data.NewNumberDataPoint(
						uint64(t.Timestamp),
						data.WithNumberDataPointMap(attributes),
						data.WithNumberDataPointDoubleValue(pct.Float),
					)),
				))
			}

		case false:
			// Computing Timers as Histograms
			opts := []func(data.HistogramDataPoint){
				data.WithHistogramDataPointStatistics(t.Values),
				data.WithHistogramDataPointAttributes(attributes),
			}
			if len(t.Histogram) != 0 {
				opts = append(opts, data.WithHistogramDataPointCumulativeBucketValues(t.Histogram))
			}
			group.Insert(bd.is, resources, data.NewMetric(name).SetHistogram(
				data.NewHistogram(data.NewHistogramDataPoint(uint64(t.Timestamp), opts...)),
			))
		}

	})

	err := bd.postMetrics(ctx, group.Values())
	if err != nil {
		bd.logger.WithError(err).WithFields(logrus.Fields{
			"endpoint": bd.endpoint,
		}).Error("Issues trying to submit data")
	}
	cb(multierr.Errors(err))
}

func (c *Backend) postMetrics(ctx context.Context, resourceMetrics []data.ResourceMetrics) error {
	req, err := data.NewMetricsRequest(ctx, c.endpoint, resourceMetrics...)
	if err != nil {
		atomic.AddUint64(&c.droppedMetrics, uint64(len(resourceMetrics)))
		return err
	}

	c.metricRequestsBufferSem <- struct{}{}
	resp, err := c.client.Do(req)
	<-c.metricRequestsBufferSem
	if err != nil {
		atomic.AddUint64(&c.droppedMetrics, uint64(len(resourceMetrics)))
		return err
	}
	dropped, err := data.ProcessMetricResponse(resp)
	atomic.AddUint64(&c.droppedMetrics, uint64(dropped))
	return err
}
