package promremotewriter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/tilinna/clock"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/util"
	"github.com/atlassian/gostatsd/pb"
	"github.com/atlassian/gostatsd/pkg/stats"
	"github.com/atlassian/gostatsd/pkg/transport"
)

const (
	// BackendName is the name of this backend.
	BackendName                  = "promremotewriter"
	defaultUserAgent             = "gostatsd" // TODO: Add version
	defaultMaxRequestElapsedTime = 15 * time.Second
	// defaultMetricsPerBatch is the default number of metrics to send in a single batch.
	defaultMetricsPerBatch = 1000
	// maxResponseSize is the maximum response size we are willing to read.
	maxResponseSize = 1024
)

var (
	// defaultMaxRequests is the number of parallel outgoing requests to prometheus.  As this mixes
	// both CPU (TLS) and network bound operations, balancing may require some experimentation.
	defaultMaxRequests = uint(10 * runtime.NumCPU())
)

// Client represents a Prometheus Remote Writer client.
type Client struct {
	batchesCreated uint64            // Accumulated number of batches created
	batchesDropped uint64            // Accumulated number of batches aborted (data loss)
	batchesSent    uint64            // Accumulated number of batches successfully sent
	seriesSent     uint64            // Accumulated number of series successfully sent
	batchesRetried stats.ChangeGauge // Accumulated number of batches retried (first send is not a retry)

	logger                logrus.FieldLogger
	apiEndpoint           string
	userAgent             string
	maxRequestElapsedTime time.Duration
	client                *http.Client
	metricsPerBatch       uint
	metricsBufferSem      chan *bytes.Buffer // Two in one - a semaphore and a buffer pool

	disabledSubtypes gostatsd.TimerSubtypes
}

// SendMetricsAsync flushes the metrics to Prometheus, preparing payload synchronously but doing the send asynchronously.
func (prw *Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	counter := 0
	results := make(chan error)

	now := clock.FromContext(ctx).Now().UnixNano() / 1_000_000
	prw.processMetrics(now, metrics, func(writeReq *pb.PromWriteRequest) {
		atomic.AddUint64(&prw.batchesCreated, 1)
		go func() {
			select {
			case <-ctx.Done():
				return
			case buffer := <-prw.metricsBufferSem:
				err := prw.postMetrics(ctx, &buffer, writeReq)

				select {
				case <-ctx.Done():
				case results <- err:
				}

				buffer.Reset()
				prw.metricsBufferSem <- buffer
			}
		}()
		counter++
	})
	go func() {
		errs := make([]error, 0, counter)
	loop:
		for c := 0; c < counter; c++ {
			select {
			case <-ctx.Done():
				errs = append(errs, ctx.Err())
				break loop
			case err := <-results:
				errs = append(errs, err)
			}
		}
		cb(errs)
	}()
}

func (prw *Client) Run(ctx context.Context) {
	statser := stats.FromContext(ctx).WithTags(gostatsd.Tags{"backend:promremotewriter"})

	flushed, unregister := statser.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushed:
			statser.Gauge("backend.created", float64(atomic.LoadUint64(&prw.batchesCreated)), nil)
			prw.batchesRetried.SendIfChanged(statser, "backend.retried", nil)
			statser.Gauge("backend.dropped", float64(atomic.LoadUint64(&prw.batchesDropped)), nil)
			statser.Gauge("backend.sent", float64(atomic.LoadUint64(&prw.batchesSent)), nil)
			statser.Gauge("backend.series.sent", float64(atomic.LoadUint64(&prw.seriesSent)), nil)
		}
	}
}

func (prw *Client) processMetrics(now int64, metrics *gostatsd.MetricMap, cb func(request *pb.PromWriteRequest)) {
	fl := flush{
		writeRequest:    &pb.PromWriteRequest{},
		timestamp:       now,
		metricsPerBatch: prw.metricsPerBatch,
		cb:              cb,
	}

	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		fl.addMetric(counter.PerSecond, counter.Tags, key)
		fl.addMetricf(float64(counter.Value), counter.Tags, "%s.count", key)
		fl.maybeFlush()
	})

	metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		if timer.Histogram != nil {
			for histogramThreshold, count := range timer.Histogram {
				bucketTag := "le:+Inf"
				if !math.IsInf(float64(histogramThreshold), 1) {
					bucketTag = "le:" + strconv.FormatFloat(float64(histogramThreshold), 'f', -1, 64)
				}
				newTags := timer.Tags.Concat(gostatsd.Tags{bucketTag})
				fl.addMetricf(float64(count), newTags, "%s.histogram", key)
			}
		} else {

			if !prw.disabledSubtypes.Lower {
				fl.addMetricf(timer.Min, timer.Tags, "%s.lower", key)
			}
			if !prw.disabledSubtypes.Upper {
				fl.addMetricf(timer.Max, timer.Tags, "%s.upper", key)
			}
			if !prw.disabledSubtypes.Count {
				fl.addMetricf(float64(timer.Count), timer.Tags, "%s.count", key)
			}
			if !prw.disabledSubtypes.CountPerSecond {
				fl.addMetricf(timer.PerSecond, timer.Tags, "%s.count_ps", key)
			}
			if !prw.disabledSubtypes.Mean {
				fl.addMetricf(timer.Mean, timer.Tags, "%s.mean", key)
			}
			if !prw.disabledSubtypes.Median {
				fl.addMetricf(timer.Median, timer.Tags, "%s.median", key)
			}
			if !prw.disabledSubtypes.StdDev {
				fl.addMetricf(timer.StdDev, timer.Tags, "%s.std", key)
			}
			if !prw.disabledSubtypes.Sum {
				fl.addMetricf(timer.Sum, timer.Tags, "%s.sum", key)
			}
			if !prw.disabledSubtypes.SumSquares {
				fl.addMetricf(timer.SumSquares, timer.Tags, "%s.sum_squares", key)
			}
			for _, pct := range timer.Percentiles {
				fl.addMetricf(pct.Float, timer.Tags, "%s.%s", key, pct.Str)
			}
		}
		fl.maybeFlush()
	})

	metrics.Gauges.Each(func(key, tagsKey string, g gostatsd.Gauge) {
		fl.addMetric(g.Value, g.Tags, key)
		fl.maybeFlush()
	})

	metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		fl.addMetric(float64(len(set.Values)), set.Tags, key)
		fl.maybeFlush()
	})

	fl.finish()
}

func (prw *Client) postMetrics(ctx context.Context, buffer **bytes.Buffer, writeReq *pb.PromWriteRequest) error {
	if err := prw.post(ctx, buffer, "metrics", writeReq); err != nil {
		return err
	}
	atomic.AddUint64(&prw.seriesSent, uint64(len(writeReq.Timeseries)))
	return nil
}

// SendEvent sends an event to NOWHERE.
func (prw *Client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	return nil
}

// Name returns the name of the backend.
func (prw *Client) Name() string {
	return BackendName
}

func (prw *Client) post(ctx context.Context, buffer **bytes.Buffer, typeOfPost string, writeReq *pb.PromWriteRequest) error {
	post, err := prw.constructPost(ctx, buffer, writeReq)
	if err != nil {
		atomic.AddUint64(&prw.batchesDropped, 1)
		return err
	}

	b := backoff.NewExponentialBackOff()
	clck := clock.FromContext(ctx)
	b.Clock = clck
	b.Reset()
	b.MaxElapsedTime = prw.maxRequestElapsedTime
	for {
		if err = post(); err == nil {
			atomic.AddUint64(&prw.batchesSent, 1)
			return nil
		}

		next := b.NextBackOff()
		if next == backoff.Stop {
			atomic.AddUint64(&prw.batchesDropped, 1)
			return fmt.Errorf("[%s] %v", BackendName, err)
		}

		prw.logger.WithFields(logrus.Fields{
			"type":  typeOfPost,
			"sleep": next,
			"error": err,
		}).Warn("failed to send")

		timer := clck.NewTimer(next)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		atomic.AddUint64(&prw.batchesRetried.Cur, 1)
	}
}

func (prw *Client) constructPost(ctx context.Context, buffer **bytes.Buffer, writeReq *pb.PromWriteRequest) (func() error /*doPost*/, error) {
	raw, err := proto.Marshal(writeReq)
	if err != nil {
		return nil, err
	}

	bufBorrowed := (*buffer).Bytes()
	bufCompressed := snappy.Encode(bufBorrowed, raw)
	if cap(bufBorrowed) < cap(bufCompressed) {
		// it overflowed the buffer and had to allocate, so we'll keep the larger buffer for next time
		*buffer = bytes.NewBuffer(bufCompressed)
	}

	return func() error {
		req, err := http.NewRequest("POST", prw.apiEndpoint, bytes.NewReader(bufCompressed))
		if err != nil {
			return fmt.Errorf("unable to create http.Request: %v", err)
		}
		req = req.WithContext(ctx)

		headers := map[string]string{
			"Content-type":     "application/x-protobuf",
			"Content-Encoding": "snappy",
			"User-Agent":       prw.userAgent,
		}
		for header, v := range headers {
			req.Header.Set(header, v)
		}

		resp, err := prw.client.Do(req)
		if err != nil {
			return fmt.Errorf("error POSTing: %s", err)
		}
		defer resp.Body.Close()
		body := io.LimitReader(resp.Body, maxResponseSize)
		if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusNoContent {
			b, _ := ioutil.ReadAll(body)
			prw.logger.WithFields(logrus.Fields{
				"status": resp.StatusCode,
				"body":   string(b),
			}).Info("request failed")
			return fmt.Errorf("received bad status code %d", resp.StatusCode)
		}
		_, _ = io.Copy(ioutil.Discard, body)
		return nil
	}, nil
}

// NewClientFromViper returns a new Prometheus Remote Writer client.
func NewClientFromViper(v *viper.Viper, logger logrus.FieldLogger, pool *transport.TransportPool) (gostatsd.Backend, error) {
	prw := util.GetSubViper(v, "promremotewriter")
	prw.SetDefault("metrics-per-batch", defaultMetricsPerBatch)
	prw.SetDefault("max-request-elapsed-time", defaultMaxRequestElapsedTime)
	prw.SetDefault("max-requests", defaultMaxRequests)
	prw.SetDefault("user-agent", defaultUserAgent)
	prw.SetDefault("transport", "default")

	return NewClient(
		prw.GetString("api-endpoint"),
		prw.GetString("user-agent"),
		prw.GetString("transport"),
		prw.GetInt("metrics-per-batch"),
		uint(prw.GetInt("max-requests")),
		prw.GetDuration("max-request-elapsed-time"),
		gostatsd.DisabledSubMetrics(v),
		logger,
		pool,
	)
}

// NewClient returns a new Prometheus Remote Writer API client.
func NewClient(
	apiEndpoint,
	userAgent,
	transport string,
	metricsPerBatch int,
	maxRequests uint,
	maxRequestElapsedTime time.Duration,
	disabled gostatsd.TimerSubtypes,
	logger logrus.FieldLogger,
	pool *transport.TransportPool,
) (*Client, error) {
	if apiEndpoint == "" {
		return nil, fmt.Errorf("[%s] api-endpoint is required", BackendName)
	}
	if userAgent == "" {
		return nil, fmt.Errorf("[%s] user-agent is required", BackendName)
	}
	if metricsPerBatch <= 0 {
		return nil, fmt.Errorf("[%s] metrics-per-batch must be positive", BackendName)
	}
	if maxRequestElapsedTime <= 0 && maxRequestElapsedTime != -1 {
		return nil, fmt.Errorf("[%s] max-request-elapsed-time must be positive", BackendName)
	}

	httpClient, err := pool.Get(transport)
	if err != nil {
		logger.WithError(err).Error("failed to create http client")
		return nil, err
	}
	logger.WithFields(logrus.Fields{
		"max-request-elapsed-time": maxRequestElapsedTime,
		"max-requests":             maxRequests,
		"metrics-per-batch":        metricsPerBatch,
	}).Info("created backend")

	metricsBufferSem := make(chan *bytes.Buffer, maxRequests)
	for i := uint(0); i < maxRequests; i++ {
		metricsBufferSem <- &bytes.Buffer{}
	}
	return &Client{
		logger:                logger,
		apiEndpoint:           apiEndpoint,
		userAgent:             userAgent,
		maxRequestElapsedTime: maxRequestElapsedTime,
		client:                httpClient.Client,
		metricsPerBatch:       uint(metricsPerBatch),
		metricsBufferSem:      metricsBufferSem,
		disabledSubtypes:      disabled,
	}, nil
}
