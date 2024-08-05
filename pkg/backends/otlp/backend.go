package otlp

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.uber.org/multierr"

	"github.com/atlassian/gostatsd/pkg/stats"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/backends/otlp/internal/data"
	"github.com/atlassian/gostatsd/pkg/transport"
)

const (
	BackendName = `otlp`
)

// Backend contains additional meta data in order
// to export values as OTLP metrics.
// The zero value is not safe to use.
type Backend struct {
	batchesCreated uint64 // Accumulated number of batches created
	batchesDropped uint64 // Accumulated number of batches aborted (data loss)
	batchesSent    uint64 // Accumulated number of batches successfully sent
	seriesSent     uint64 // Accumulated number of series successfully sent
	seriesDropped  uint64 // Accumulated number of series aborted (data loss)

	eventsSent    uint64 // Accumulated number of events successfully sent
	eventsDropped uint64 // Accumulated number of events aborted (data loss)

	metricsEndpoint       string
	logsEndpoint          string
	convertTimersToGauges bool
	is                    data.InstrumentationScope
	resourceKeys          gostatsd.Tags

	discarded         gostatsd.TimerSubtypes
	logger            logrus.FieldLogger
	client            *http.Client
	requestsBufferSem chan struct{}

	// metricsPerBatch is the maximum number of metrics to send in a single batch.
	metricsPerBatch int
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
		metricsEndpoint:       cfg.MetricsEndpoint,
		logsEndpoint:          cfg.LogsEndpoint,
		convertTimersToGauges: cfg.Conversion == ConversionAsGauge,
		is:                    data.NewInstrumentationScope("gostatsd/aggregation", version),
		resourceKeys:          cfg.ResourceKeys,
		discarded:             cfg.TimerSubtypes,
		client:                tc.Client,
		logger:                logger,
		requestsBufferSem:     make(chan struct{}, cfg.MaxRequests),
		metricsPerBatch:       cfg.MetricsPerBatch,
	}, nil
}

func (*Backend) Name() string {
	return BackendName
}

func (b *Backend) SendEvent(ctx context.Context, event *gostatsd.Event) error {
	statser := stats.FromContext(ctx).WithTags(gostatsd.Tags{"backend:otlp"})
	defer func() {
		statser.Gauge("backend.created", float64(atomic.LoadUint64(&b.batchesCreated)), nil)
		statser.Gauge("backend.dropped", float64(atomic.LoadUint64(&b.batchesDropped)), nil)
		statser.Gauge("backend.sent", float64(atomic.LoadUint64(&b.batchesSent)), nil)
		statser.Gauge("backend.series.sent", float64(atomic.LoadUint64(&b.seriesSent)), nil)
		statser.Gauge("backend.series.dropped", float64(atomic.LoadUint64(&b.seriesDropped)), nil)

		statser.Gauge("backend.dropped_events", float64(atomic.LoadUint64(&b.eventsDropped)), nil)
		statser.Gauge("backend.sent_events", float64(atomic.LoadUint64(&b.eventsSent)), nil)
	}()

	se, err := data.NewOtlpEvent(
		event,
	)
	if err != nil {
		return err
	}

	el, rt := se.TransformToLog(b.resourceKeys)

	req, err := data.NewEventsRequest(ctx, b.logsEndpoint, el, rt)
	if err != nil {
		atomic.AddUint64(&b.eventsDropped, 1)
		return err
	}

	b.requestsBufferSem <- struct{}{}
	resp, err := b.client.Do(req)
	<-b.requestsBufferSem
	if err != nil {
		atomic.AddUint64(&b.eventsDropped, 1)
		return err
	}

	err = data.ProcessEventsResponse(resp)
	atomic.AddUint64(&b.eventsDropped, 1)

	return err
}

func (bd *Backend) SendMetricsAsync(ctx context.Context, mm *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	statser := stats.FromContext(ctx).WithTags(gostatsd.Tags{"backend:otlp"})
	defer func() {
		statser.Gauge("backend.dropped", float64(atomic.LoadUint64(&bd.batchesDropped)), nil)
		statser.Gauge("backend.sent", float64(atomic.LoadUint64(&bd.batchesSent)), nil)
	}()

	group := newGroups(bd.metricsPerBatch)

	mm.Counters.Each(func(name, _ string, cm gostatsd.Counter) {
		resources, attributes := data.SplitMetricTagsByKeysAndConvert(cm.Tags, bd.resourceKeys)

		rate := data.NewMetric(name).SetGauge(
			data.NewGauge(
				data.NewNumberDataPoint(
					uint64(cm.Timestamp),
					data.WithNumberDataPointMap(attributes),
					data.WithNumberDataPointDoubleValue(cm.PerSecond),
				),
			),
		)

		m := data.NewMetric(name + ".count").SetSum(
			data.NewSum(
				data.NewNumberDataPoint(
					uint64(cm.Timestamp),
					data.WithNumberDataPointMap(attributes),
					data.WithNumberDatapointIntValue(cm.Value),
				),
			),
		)

		group.insert(bd.is, resources, rate)
		group.insert(bd.is, resources, m)
	})

	mm.Gauges.Each(func(name, _ string, gm gostatsd.Gauge) {
		resources, attributes := data.SplitMetricTagsByKeysAndConvert(gm.Tags, bd.resourceKeys)

		m := data.NewMetric(name).SetGauge(
			data.NewGauge(
				data.NewNumberDataPoint(
					uint64(gm.Timestamp),
					data.WithNumberDataPointMap(attributes),
					data.WithNumberDataPointDoubleValue(gm.Value),
				),
			),
		)

		group.insert(bd.is, resources, m)
	})

	mm.Sets.Each(func(name, _ string, sm gostatsd.Set) {
		resources, attributes := data.SplitMetricTagsByKeysAndConvert(sm.Tags, bd.resourceKeys)

		m := data.NewMetric(name).SetGauge(
			data.NewGauge(
				data.NewNumberDataPoint(
					uint64(sm.Timestamp),
					data.WithNumberDataPointMap(attributes),
					data.WithNumberDatapointIntValue(int64(len(sm.Values))),
				),
			),
		)

		group.insert(bd.is, resources, m)
	})

	mm.Timers.Each(func(name, _ string, t gostatsd.Timer) {
		resources, attributes := data.SplitMetricTagsByKeysAndConvert(t.Tags, bd.resourceKeys)

		switch bd.convertTimersToGauges {
		case true:
			if len(t.Histogram) != 0 {
				for boundry, count := range t.Histogram {
					btags := data.NewMap()
					btags.Merge(attributes)
					if math.IsInf(float64(boundry), 1) {
						btags.Insert("le", "+Inf")
					} else {
						btags.Insert("le", strconv.FormatFloat(float64(boundry), 'f', -1, 64))
					}
					group.insert(
						bd.is,
						resources,
						data.NewMetric(fmt.Sprintf("%s.histogram", name)).SetGauge(
							data.NewGauge(data.NewNumberDataPoint(
								uint64(t.Timestamp),
								data.WithNumberDataPointMap(btags),
								data.WithNumberDataPointDoubleValue(float64(count)),
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
				group.insert(
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
				group.insert(bd.is, resources, data.NewMetric(fmt.Sprintf("%s.%s", name, pct.Str)).SetGauge(
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
			group.insert(bd.is, resources, data.NewMetric(name).SetHistogram(
				data.NewHistogram(data.NewHistogramDataPoint(uint64(t.Timestamp), opts...)),
			))
		}
	})

	var errs error
	for _, b := range group.batches {
		atomic.AddUint64(&bd.batchesCreated, 1)
		err := bd.postMetrics(ctx, b)
		if err != nil {
			bd.logger.WithError(err).WithFields(logrus.Fields{
				"endpoint": bd.metricsEndpoint,
			}).Error("Issues trying to submit data")
			errs = multierr.Append(errs, err)
		} else {
			atomic.AddUint64(&bd.batchesSent, 1)
			atomic.AddUint64(&bd.seriesSent, uint64(b.lenMetrics()))
		}
	}
	cb(multierr.Errors(errs))
}

func (c *Backend) postMetrics(ctx context.Context, batch group) error {
	resourceMetrics := batch.values()

	req, err := data.NewMetricsRequest(ctx, c.metricsEndpoint, resourceMetrics)
	if err != nil {
		atomic.AddUint64(&c.batchesDropped, 1)
		return err
	}

	c.requestsBufferSem <- struct{}{}
	resp, err := c.client.Do(req)
	<-c.requestsBufferSem
	if err != nil {
		atomic.AddUint64(&c.batchesDropped, 1)
		return err
	}
	dropped, err := data.ProcessMetricResponse(resp)
	atomic.AddUint64(&c.seriesDropped, uint64(dropped))

	return err
}
