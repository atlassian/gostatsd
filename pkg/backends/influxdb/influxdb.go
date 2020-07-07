package influxdb

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/tilinna/clock"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/util"
	"github.com/atlassian/gostatsd/pkg/stats"
	"github.com/atlassian/gostatsd/pkg/transport"
)

const (
	// BackendName is the name of this backend.
	BackendName                  = "influxdb"
	defaultMaxRequestElapsedTime = 15 * time.Second
	// defaultMetricsPerBatch is the default number of metrics to send in a single request.
	// - https://docs.influxdata.com/influxdb/v1.8/tools/api/#request-body-1 says 5000-10000 is optimal (v1.8)
	// - https://v2.docs.influxdata.com/v2.0/write-data/best-practices/optimize-writes/#batch-writes says 5000 is optimal (v2)
	defaultMetricsPerBatch = 5000
	// maxResponseSize is the maximum response size we are willing to read.
	maxResponseSize = 1024

	paramApiEndpoint           = "api-endpoint"
	paramCompressPayload       = "compress-payload"
	paramCredentials           = "credentials"
	paramMaxRequestElapsedTime = "max-request-elapsed-time"
	paramMaxRequests           = "max-requests"
	paramMetricsPerBatch       = "metrics-per-batch"
	paramTransport             = "transport"

	queryPrecision = "precision"
)

var (
	// defaultMaxRequests is the number of parallel outgoing requests to InfluxDB.  This is primarily
	// network I/O, not CPU.
	defaultMaxRequests = uint(10 * runtime.NumCPU())

	errApiEndpointRequired          = errors.New("[" + BackendName + "] " + paramApiEndpoint + " is required")
	errMaxRequestsIsNotPositive     = errors.New("[" + BackendName + "] " + paramMaxRequests + " must be above zero")
	errMaxRequestElapsedTimeInvalid = errors.New("[" + BackendName + "] " + paramMaxRequestElapsedTime + " must be positive or -1")
	errMetricsPerBatchIsNotPositive = errors.New("[" + BackendName + "] " + paramMetricsPerBatch + " must be positive")
	errPostEventFailed              = errors.New("[" + BackendName + "] failed to post event")
)

// Client represents an InfluxDB client.
type Client struct {
	batchesCreated      uint64 // Accumulated number of batches created
	batchesRetried      uint64 // Accumulated number of batches retried (first send is not a retry)
	batchesDropped      uint64 // Accumulated number of batches aborted (data loss)
	batchesCreateFailed uint64 // Accumulated number of batches which failed to serialize (data loss, no retry is possible)
	batchesSent         uint64 // Accumulated number of batches successfully sent
	seriesSent          uint64 // Accumulated number of series successfully sent

	logger logrus.FieldLogger

	credentials string
	url         string

	maxRequestElapsedTime time.Duration
	client                *http.Client
	metricsPerBatch       uint64
	reqBufferSem          chan *bytes.Buffer // Two in one - a semaphore and a buffer pool
	compressPayload       bool

	disabledSubtypes gostatsd.TimerSubtypes
	flushInterval    time.Duration
}

// NewClientFromViper returns a new InfluxDB API client.
func NewClientFromViper(
	v *viper.Viper,
	logger logrus.FieldLogger,
	pool *transport.TransportPool,
) (gostatsd.Backend, error) {
	influxViper := util.GetSubViper(v, "influxdb")
	influxViper.SetDefault(paramApiEndpoint, "")
	influxViper.SetDefault(paramCompressPayload, true)
	influxViper.SetDefault(paramCredentials, "")
	influxViper.SetDefault(paramMaxRequestElapsedTime, defaultMaxRequestElapsedTime)
	influxViper.SetDefault(paramMaxRequests, defaultMaxRequests)
	influxViper.SetDefault(paramMetricsPerBatch, defaultMetricsPerBatch)
	influxViper.SetDefault(paramTransport, "default")

	cfg, err := newConfigFromViper(influxViper, logger)
	if err != nil {
		return nil, err
	}

	return NewClient(
		influxViper.GetString(paramApiEndpoint),
		influxViper.GetBool(paramCompressPayload),
		influxViper.GetString(paramCredentials),
		influxViper.GetUint(paramMaxRequests),
		influxViper.GetDuration(paramMaxRequestElapsedTime),
		influxViper.GetUint64(paramMetricsPerBatch),
		influxViper.GetString(paramTransport),
		cfg,
		gostatsd.DisabledSubMetrics(v),
		logger,
		pool,
	)
}

// NewClient returns a new InfluxDB API client.
func NewClient(
	apiEndpoint string,
	compressPayload bool,
	credentials string,
	maxRequests uint,
	maxRequestElapsedTime time.Duration,
	metricsPerBatch uint64,
	transport string,
	cfg config,
	disabled gostatsd.TimerSubtypes,
	logger logrus.FieldLogger,
	pool *transport.TransportPool,
) (*Client, error) {

	if apiEndpoint == "" {
		return nil, errApiEndpointRequired
	}
	if maxRequests == 0 {
		return nil, errMaxRequestsIsNotPositive
	}
	if maxRequestElapsedTime <= 0 && maxRequestElapsedTime != -1 {
		return nil, errMaxRequestElapsedTimeInvalid
	}
	if metricsPerBatch == 0 {
		return nil, errMetricsPerBatchIsNotPositive
	}
	parsedEndpoint, err := url.Parse(apiEndpoint)
	if err != nil {
		logger.WithError(err).Error(paramApiEndpoint + " is not valid")
		return nil, err
	}

	httpClient, err := pool.Get(transport)
	if err != nil {
		logger.WithError(err).Error("failed to create http client")
		return nil, err
	}

	reqBufferSem := make(chan *bytes.Buffer, maxRequests)
	for i := uint(0); i < maxRequests; i++ {
		reqBufferSem <- &bytes.Buffer{}
	}

	query := parsedEndpoint.Query()
	query.Set(queryPrecision, "s")
	cfg.Build(query)
	parsedEndpoint.Path = path.Join(parsedEndpoint.Path, cfg.Path())
	parsedEndpoint.RawQuery = query.Encode()

	creationFields := logrus.Fields{
		paramApiEndpoint:           apiEndpoint,
		paramCompressPayload:       compressPayload,
		paramMaxRequests:           maxRequests,
		paramMaxRequestElapsedTime: maxRequestElapsedTime,
		paramMetricsPerBatch:       metricsPerBatch,
		paramTransport:             transport,
	}

	if credentials != "" {
		creationFields[paramCredentials] = "(set)"
	} else {
		creationFields[paramCredentials] = "(unset)"
	}

	logger.WithFields(creationFields).Info("created backend")

	return &Client{
		logger:                logger,
		url:                   parsedEndpoint.String(),
		compressPayload:       compressPayload,
		credentials:           credentials,
		maxRequestElapsedTime: maxRequestElapsedTime,
		metricsPerBatch:       metricsPerBatch,
		client:                httpClient.Client,
		reqBufferSem:          reqBufferSem,
		disabledSubtypes:      disabled,
	}, nil
}

func (idb *Client) getBuffer(ctx context.Context) (*bytes.Buffer, io.WriteCloser) {
	select {
	case <-ctx.Done():
		return nil, nil
	case buf := <-idb.reqBufferSem:
		if !idb.compressPayload {
			return buf, util.NopWriteCloser(buf)
		}
		// No error check, per the documentation: "The error returned will be nil if the level is valid."
		// We may need to re-assess this if we make the level user configurable.
		g, _ := gzip.NewWriterLevel(buf, gzip.BestCompression)
		return buf, g
	}
}

func (idb *Client) releaseBuffer(buf *bytes.Buffer) {
	buf.Reset()
	idb.reqBufferSem <- buf
}

// SendMetricsAsync flushes the metrics to Influxdb, preparing payload synchronously but doing the send asynchronously.
func (idb *Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	counter := 0
	results := make(chan error)

	now := clock.FromContext(ctx).Now().Unix()
	idb.processMetrics(ctx, now, metrics, func(buf *bytes.Buffer, seriesCount uint64) {
		atomic.AddUint64(&idb.batchesCreated, 1)
		go func() {
			err := idb.postData(ctx, buf, seriesCount)
			idb.releaseBuffer(buf)
			select {
			case <-ctx.Done():
			case results <- err:
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

func (idb *Client) Run(ctx context.Context) {
	statser := stats.FromContext(ctx).WithTags(gostatsd.Tags{"backend:influxdb"})

	flushed, unregister := statser.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushed:
			statser.Gauge("backend.created", float64(atomic.LoadUint64(&idb.batchesCreated)), nil)
			statser.Gauge("backend.create.failed", float64(atomic.LoadUint64(&idb.batchesCreateFailed)), nil)
			statser.Gauge("backend.retried", float64(atomic.LoadUint64(&idb.batchesRetried)), nil)
			statser.Gauge("backend.dropped", float64(atomic.LoadUint64(&idb.batchesDropped)), nil)
			statser.Gauge("backend.sent", float64(atomic.LoadUint64(&idb.batchesSent)), nil)
			statser.Gauge("backend.series.sent", float64(atomic.LoadUint64(&idb.seriesSent)), nil)
		}
	}
}

func (idb *Client) processMetrics(ctx context.Context, nowSeconds int64, metrics *gostatsd.MetricMap, cb func(buf *bytes.Buffer, seriesCount uint64)) {
	fl := flush{
		timestampSeconds: nowSeconds,
		flushIntervalSec: idb.flushInterval.Seconds(),
		metricsPerBatch:  idb.metricsPerBatch,
		disabledSubtypes: idb.disabledSubtypes,
		errorCounter:     &idb.batchesCreateFailed,
		cb:               cb,
		getBuffer: func() (*bytes.Buffer, io.WriteCloser) {
			return idb.getBuffer(ctx)
		},
		releaseBuffer: idb.releaseBuffer,
	}

	fl.buffer, fl.writer = fl.getBuffer()

	metrics.Counters.Each(func(metricName, tagsKey string, counter gostatsd.Counter) {
		if fl.buffer == nil {
			return
		}
		fl.addCounter(metricName, counter.Tags, counter.Value, counter.PerSecond)
	})

	metrics.Timers.Each(func(metricName, tagsKey string, timer gostatsd.Timer) {
		if fl.buffer == nil {
			return
		}
		if timer.Histogram == nil {
			fl.addBaseTimer(metricName, timer)
		} else {
			fl.addHistogramTimer(metricName, timer)
		}
	})

	metrics.Gauges.Each(func(metricName, tagsKey string, g gostatsd.Gauge) {
		if fl.buffer == nil {
			return
		}
		fl.addGauge(metricName, g.Tags, g.Value)
	})

	metrics.Sets.Each(func(metricName, tagsKey string, set gostatsd.Set) {
		if fl.buffer == nil {
			return
		}
		fl.addSet(metricName, set.Tags, uint64(len(set.Values)))
	})

	if fl.metricCount == 0 {
		idb.releaseBuffer(fl.buffer)
		return
	}

	fl.finish()
}

func (idb *Client) postData(ctx context.Context, buffer *bytes.Buffer, seriesCount uint64) error {
	if err := idb.post(ctx, buffer); err != nil {
		return err
	}
	atomic.AddUint64(&idb.seriesSent, seriesCount)
	return nil
}

func writeEvent(w io.Writer, e *gostatsd.Event) {
	hasHost := false
	var eventTags []string
	tags := make(gostatsd.Tags, 0, len(e.Tags)+4) // for priority, alerttype, etc below
	for _, tag := range e.Tags {
		if strings.HasPrefix(tag, "host:") {
			hasHost = true
		}
		if !strings.HasPrefix(tag, "eventtags") {
			tags = append(tags, tag)
		} else {
			parts := strings.SplitN(tag, ":", 2)
			if len(parts) > 1 {
				eventTags = append(eventTags, parts[1])
			}
		}
	}

	// We don't need to use .Concat(), because we created the original slice.
	tags = append(
		tags,
		"priority:"+e.Priority.String(),
		"alerttype:"+e.AlertType.String(),
	)
	if e.SourceTypeName != "" {
		tags = append(tags, "sourcetypename:"+e.SourceTypeName)
	}
	if e.Hostname != "" && !hasHost {
		tags = append(tags, "host:"+string(e.Hostname))
	}

	writeName(w, "events", tags)

	sb := &strings.Builder{}

	sb.WriteString("title=")
	escapeStringToBuilder(sb, e.Title)
	sb.WriteString(",text=")
	escapeStringToBuilder(sb, e.Text)
	if len(eventTags) > 0 {
		sb.WriteString(",tags=")
		escapeStringToBuilder(sb, strings.Join(eventTags, ","))
	}
	sb.WriteString(fmt.Sprintf(" %d\n", e.DateHappened))
	_, _ = w.Write([]byte(sb.String()))
}

func (idb *Client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	buf, writer := idb.getBuffer(ctx)
	if buf == nil {
		return ctx.Err()
	}
	defer idb.releaseBuffer(buf)

	writeEvent(writer, e)

	err := writer.Close()
	if err != nil {
		return fmt.Errorf("[%s] failed to flush event buffer: %v", BackendName, err)
	}

	err = idb.postData(ctx, buf, 0)
	if err != nil {
		return errPostEventFailed
	}

	return nil
}

// Name returns the name of the backend.
func (idb *Client) Name() string {
	return BackendName
}

func (idb *Client) newBackoff(clck clock.Clock) backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.Clock = clck
	b.Reset() // NewExponentialBackOff uses time.Now(), this re-inits with clck.Now()
	b.MaxElapsedTime = idb.maxRequestElapsedTime
	return b
}

func (idb *Client) post(ctx context.Context, buffer *bytes.Buffer) error {
	post, err := idb.constructPost(ctx, buffer)
	if err != nil {
		atomic.AddUint64(&idb.batchesDropped, 1)
		return err
	}

	clck := clock.FromContext(ctx)
	bo := idb.newBackoff(clck)
	for {
		if err = post(); err == nil {
			atomic.AddUint64(&idb.batchesSent, 1)
			return nil
		}

		next := bo.NextBackOff()
		if next == backoff.Stop {
			atomic.AddUint64(&idb.batchesDropped, 1)
			return fmt.Errorf("[%s] %v", BackendName, err)
		}

		idb.logger.WithFields(logrus.Fields{
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

		atomic.AddUint64(&idb.batchesRetried, 1)
	}
}

func (idb *Client) constructPost(ctx context.Context, buffer *bytes.Buffer) (func() error /*doPost*/, error) {
	body := buffer.Bytes()

	return func() error {
		headers := map[string]string{
			"User-Agent": "gostatsd (influxdb)",
		}
		if idb.compressPayload {
			headers["Content-Encoding"] = "gzip"
		} else {
			headers["Content-Encoding"] = "identity"
		}
		if idb.credentials != "" {
			headers["Authorization"] = "Token " + idb.credentials
		}

		req, err := http.NewRequestWithContext(ctx, "POST", idb.url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("unable to create http.Request: %v", err)
		}
		for header, v := range headers {
			req.Header.Set(header, v)
		}
		resp, err := idb.client.Do(req)
		if err != nil {
			return fmt.Errorf("error POSTing: %v", err)
		}
		defer resp.Body.Close()
		body := io.LimitReader(resp.Body, maxResponseSize)
		if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusNoContent {
			b, _ := ioutil.ReadAll(body)
			idb.logger.WithFields(logrus.Fields{
				"status": resp.StatusCode,
				"body":   string(b),
			}).Info("request failed")
			return fmt.Errorf("received bad status code %d", resp.StatusCode)
		}
		_, _ = io.Copy(ioutil.Discard, body)
		return nil
	}, nil
}
