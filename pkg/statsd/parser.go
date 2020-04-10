package statsd

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/pool"
	"github.com/atlassian/gostatsd/pkg/stats"

	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

// Default buffer size for debug channel
const logRawMetricChannelBufferSize = 1000

// DatagramParser receives datagrams and parses them into Metrics/Events
// For each Metric/Event it calls Handler.HandleMetric/Event()
type DatagramParser struct {
	// Counter fields below must be read/written only using atomic instructions.
	// 64-bit fields must be the first fields in the struct to guarantee proper memory alignment.
	// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	badLines        uint64
	metricsReceived uint64
	eventsReceived  uint64

	ignoreHost bool
	handler    gostatsd.PipelineHandler
	namespace  string // Namespace to prefix all metrics

	metricPool *pool.MetricPool

	badLineLimiter *rate.Limiter

	in <-chan []*Datagram // Input chan of datagram batches to parse

	logRawMetric         bool
	logRawMetricInitOnce sync.Once
	logRawMetricChan     chan []*gostatsd.Metric
}

// NewDatagramParser initialises a new DatagramParser.
func NewDatagramParser(in <-chan []*Datagram, ns string, ignoreHost bool, estimatedTags int, handler gostatsd.PipelineHandler, badLineRateLimitPerSecond rate.Limit, logRawMetric bool) *DatagramParser {
	limiter := &rate.Limiter{}
	if badLineRateLimitPerSecond > 0 {
		limiter = rate.NewLimiter(badLineRateLimitPerSecond, 1)
	}

	return &DatagramParser{
		in:             in,
		ignoreHost:     ignoreHost,
		handler:        handler,
		namespace:      ns,
		metricPool:     pool.NewMetricPool(estimatedTags + handler.EstimatedTags()),
		badLineLimiter: limiter,
		logRawMetric:   logRawMetric,
	}
}

func (dp *DatagramParser) RunMetricsContext(ctx context.Context) {
	statser := stats.FromContext(ctx)
	flushed, unregister := statser.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushed:
			statser.Gauge("parser.metrics_received", float64(atomic.LoadUint64(&dp.metricsReceived)), nil)
			statser.Gauge("parser.events_received", float64(atomic.LoadUint64(&dp.eventsReceived)), nil)
			statser.Gauge("parser.bad_lines_seen", float64(atomic.LoadUint64(&dp.badLines)), nil)
		}
	}
}

func (dp *DatagramParser) Run(ctx context.Context) {
	dp.initLogRawMetric(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case dgs := <-dp.in:
			var metrics []*gostatsd.Metric

			accumB, accumE := uint64(0), uint64(0)
			for _, dg := range dgs {
				// TODO: Dispatch Events in Run, not handleDatagram, so it's consistent with Metrics
				parsedMetrics, eventCount, badLineCount := dp.handleDatagram(ctx, dg.Timestamp, dg.IP, dg.Msg)
				dg.DoneFunc()
				metrics = append(metrics, parsedMetrics...)
				accumE += eventCount
				accumB += badLineCount
			}
			if len(metrics) > 0 {
				dp.handler.DispatchMetrics(ctx, metrics)

				dp.doLogRawMetric(metrics)
			}
			atomic.AddUint64(&dp.metricsReceived, uint64(len(metrics)))
			atomic.AddUint64(&dp.eventsReceived, accumE)
			atomic.AddUint64(&dp.badLines, accumB)
		}
	}
}

// logBadLineRateLimited will log a line which failed to decode, if the current rate limit has not been exceeded.
func (dp *DatagramParser) logBadLineRateLimited(line []byte, ip gostatsd.IP, err error) {
	if dp.badLineLimiter.Allow() {
		log.Infof("Error parsing line %q from %s: %v", line, ip, err)
	}
}

// handleDatagram handles the contents of a datagram and parsers it in to Metrics (which are returned), or
// Events (which are sent to the pipeline via DispatchEvent).
func (dp *DatagramParser) handleDatagram(ctx context.Context, now gostatsd.Nanotime, ip gostatsd.IP, msg []byte) (metrics []*gostatsd.Metric, eventCount uint64, badLineCount uint64) {
	var numEvents, numBad uint64
	for {
		idx := bytes.IndexByte(msg, '\n')
		var line []byte
		// protocol does not require line to end in \n
		if idx == -1 { // \n not found
			if len(msg) == 0 {
				break
			}
			line = msg
			msg = nil
		} else { // usual case
			line = msg[:idx]
			msg = msg[idx+1:]
		}
		metric, event, err := dp.parseLine(line)
		if err != nil {
			// logging as debug to avoid spamming logs when a bad actor sends
			// badly formatted messages
			dp.logBadLineRateLimited(line, ip, err)
			numBad++
			continue
		}
		if metric != nil {
			if dp.ignoreHost {
				for idx, tag := range metric.Tags {
					if strings.HasPrefix(tag, "host:") {
						metric.Hostname = tag[5:]
						if len(metric.Tags) > 1 {
							metric.Tags = append(metric.Tags[:idx], metric.Tags[idx+1:]...)
						} else {
							metric.Tags = nil
						}
						break
					}
				}
			} else {
				metric.SourceIP = ip
			}
			metric.Timestamp = now
			metrics = append(metrics, metric)
		} else if event != nil {
			numEvents++
			event.SourceIP = ip // Always keep the source ip for events
			if event.DateHappened == 0 {
				event.DateHappened = time.Now().Unix()
			}
			dp.handler.DispatchEvent(ctx, event)
		} else {
			// Should never happen.
			log.Panic("Both event and metric are nil")
		}
	}
	return metrics, numEvents, numBad
}

// parseLine with lexer.
func (dp *DatagramParser) parseLine(line []byte) (*gostatsd.Metric, *gostatsd.Event, error) {
	l := lexer{
		metricPool: dp.metricPool,
	}
	return l.run(line, dp.namespace)
}

func (dp *DatagramParser) initLogRawMetric(ctx context.Context) {
	if dp.logRawMetric {
		dp.logRawMetricInitOnce.Do(func() {
			dp.logRawMetricChan = make(chan []*gostatsd.Metric, logRawMetricChannelBufferSize)

			go func() {
				// The `StandardLogger` returns the singleton log instance which is used elsewhere in the application.
				// And the `log` package makes sure that the logging interface are thread-safe so that we wouldn't
				// mess up the contents of stdout/stderr.
				lg := log.StandardLogger()
				for {
					select {
					case <-ctx.Done():
						return
					case metrics := <-dp.logRawMetricChan:
						lg.Info(metrics)
					}
				}
			}()
		})
	}
}

func (dp *DatagramParser) doLogRawMetric(metrics []*gostatsd.Metric) {
	if dp.logRawMetric {
		// Deliver only once, we don't want the whole pipeline being blocked due to a slow terminal.
		// So may lose packet if the channel buffer is full.
		select {
		case dp.logRawMetricChan <- metrics:
		default:
		}
	}
}
