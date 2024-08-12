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
	"golang.org/x/sync/errgroup"

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
	batchesCreated uint64            // Accumulated number of batches created
	batchesDropped uint64            // Accumulated number of batches aborted (data loss)
	batchesSent    uint64            // Accumulated number of batches successfully sent
	batchesRetried stats.ChangeGauge // Accumulated number of batches retried (first send is not a retry)
	seriesSent     uint64            // Accumulated number of series successfully sent
	seriesDropped  uint64            // Accumulated number of series aborted (data loss)

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
	maxRetries        int
	CompressPayload   bool

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
		maxRetries:            cfg.MaxRetries,
		CompressPayload:       cfg.CompressPayload,
		metricsPerBatch:       cfg.MetricsPerBatch,
	}, nil
}

func (*Backend) Name() string {
	return BackendName
}

func (b *Backend) SendEvent(ctx context.Context, event *gostatsd.Event) error {
	statser := stats.FromContext(ctx).WithTags(gostatsd.Tags{"backend:otlp"})
	defer func() {
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
		statser.Gauge("backend.created", float64(atomic.LoadUint64(&bd.batchesCreated)), nil)
		statser.Gauge("backend.dropped", float64(atomic.LoadUint64(&bd.batchesDropped)), nil)
		statser.Gauge("backend.sent", float64(atomic.LoadUint64(&bd.batchesSent)), nil)
		statser.Gauge("backend.series.sent", float64(atomic.LoadUint64(&bd.seriesSent)), nil)
		statser.Gauge("backend.series.dropped", float64(atomic.LoadUint64(&bd.seriesDropped)), nil)
		bd.batchesRetried.SendIfChanged(statser, "backend.retried", nil)
	}()

	currentGroup := newGroups(bd.metricsPerBatch)

	mm.Counters.Each(func(name, _ string, cm gostatsd.Counter) {
		if !cm.Tags.Exists("host") && cm.Source != "" {
			cm.Tags = cm.Tags.Concat(gostatsd.Tags{"host:" + string(cm.Source)})
		}
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

		currentGroup.insert(bd.is, resources, rate)
		currentGroup.insert(bd.is, resources, m)
	})

	mm.Gauges.Each(func(name, _ string, gm gostatsd.Gauge) {
		if !gm.Tags.Exists("host") && gm.Source != "" {
			gm.Tags = gm.Tags.Concat(gostatsd.Tags{"host:" + string(gm.Source)})
		}
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

		currentGroup.insert(bd.is, resources, m)
	})

	mm.Sets.Each(func(name, _ string, sm gostatsd.Set) {
		if !sm.Tags.Exists("host") && sm.Source != "" {
			sm.Tags = sm.Tags.Concat(gostatsd.Tags{"host:" + string(sm.Source)})
		}
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

		currentGroup.insert(bd.is, resources, m)
	})

	mm.Timers.Each(func(name, _ string, t gostatsd.Timer) {
		if !t.Tags.Exists("host") && t.Source != "" {
			t.Tags = t.Tags.Concat(gostatsd.Tags{"host:" + string(t.Source)})
		}
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
					currentGroup.insert(
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
				currentGroup.insert(
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
				currentGroup.insert(bd.is, resources, data.NewMetric(fmt.Sprintf("%s.%s", name, pct.Str)).SetGauge(
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
			currentGroup.insert(bd.is, resources, data.NewMetric(name).SetHistogram(
				data.NewHistogram(data.NewHistogramDataPoint(uint64(t.Timestamp), opts...)),
			))
		}
	})

	eg, ectx := errgroup.WithContext(ctx)
	for _, b := range currentGroup.batches {
		atomic.AddUint64(&bd.batchesCreated, 1)
		func(currentBatch group) {
			eg.Go(func() error {
				err := bd.postMetrics(ectx, currentBatch)
				if err != nil {
					bd.logger.WithError(err).WithFields(logrus.Fields{
						"endpoint": bd.metricsEndpoint,
					}).Error("Issues trying to submit data")
					atomic.AddUint64(&bd.batchesDropped, 1)
				} else {
					atomic.AddUint64(&bd.batchesSent, 1)
					atomic.AddUint64(&bd.seriesSent, uint64(currentBatch.lenMetrics()))
				}
				return err
			})
		}(b)
	}
	cb(multierr.Errors(eg.Wait()))
}

func (c *Backend) postMetrics(ctx context.Context, batch group) error {
	var (
		retries int
		req     *http.Request
		resp    *http.Response
		err     error
	)

	resourceMetrics := batch.values()
	req, err = data.NewMetricsRequest(ctx, c.metricsEndpoint, resourceMetrics, c.CompressPayload)
	if err != nil {
		return err
	}

	for {
		var dropped int64
		c.requestsBufferSem <- struct{}{}
		resp, err = c.client.Do(req)
		<-c.requestsBufferSem
		if err == nil {
			dropped, err = data.ProcessMetricResponse(resp)
			if err == nil {
				return nil
			}
			if dropped > 0 {
				// If partial data points were dropped, it shouldn't retry
				atomic.AddUint64(&c.seriesDropped, uint64(dropped))
				return err
			}
		}

		if retries >= c.maxRetries {
			break
		}

		retries++
		atomic.AddUint64(&c.batchesRetried.Cur, 1)
	}

	return err
}
