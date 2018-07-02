package datadogexp

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/tilinna/clock"
)

type metricBatcher struct {
	logger logrus.FieldLogger

	// interval is how often the metricBatcher should wake up and request more data to process.
	interval time.Duration

	// metricsPerBatch is how many individual metrics to put in a single ddTimeSeries batch.
	metricsPerBatch int

	// requestData is used to signal to a downstream component that the metricBatcher is idle and free to
	// do work. It will never be written to in a blocking fashion.
	requestData chan<- struct{}

	// receiveData is where downstream components submit data to be processed in to batches of ddTimeSeries
	// objects.  It should be unbuffered to create back pressure.
	receiveData <-chan ddMetricMap

	// submitBody is where upstream batches of metrics are sent for further processing.  It is not opinionated
	// on whether this is buffered, but it is expected that for any single receive of receiveData, many sends
	// will occur to submitBody.
	submitBatch chan<- ddTimeSeries
}

var plz = struct{}{}

// Run is responsible for periodically getting some data, packaging it in to ddTimeSeries
// batches, and sending it upstream.
func (mb *metricBatcher) Run(ctx context.Context) {
	mb.logger.Info("Starting")
	defer mb.logger.Info("Terminating")

	t := clock.NewTicker(ctx, mb.interval)
	defer t.Stop()

	// Doing this at the start of the loop is mainly for tests
	mb.requestMoreData()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// wake up and loop
			mb.requestMoreData()
		case data := <-mb.receiveData:
			mb.processData(ctx, data)
		}
	}
}

// requestMoreData will politely signal on the requestData channel that the metricBatcher has no data left to process.
func (mb *metricBatcher) requestMoreData() {
	select {
	case mb.requestData <- plz:
	default:
	}
}

// processData will turn an indexable ddMetricMap in to ddTimeSeries batches, and sends them to the submitBatch channel.
func (mb *metricBatcher) processData(ctx context.Context, data ddMetricMap) {
	ts := ddTimeSeries{
		Series: make([]*ddMetric, 0, mb.metricsPerBatch),
	}

	for _, mm := range data {
		for _, m := range mm {
			/* maybe we reuse things at some point.
			if len(m.Points) == 0 {
				continue
			}
			*/
			ts.Series = append(ts.Series, m)
			if len(ts.Series) == mb.metricsPerBatch {
				select {
				case <-ctx.Done():
					return
				case mb.submitBatch <- ts:
				}
				ts = ddTimeSeries{
					Series: make([]*ddMetric, 0, mb.metricsPerBatch),
				}
			}
		}
	}

	if len(ts.Series) > 0 {
		select {
		case <-ctx.Done():
		case mb.submitBatch <- ts:
		}
	}
}
