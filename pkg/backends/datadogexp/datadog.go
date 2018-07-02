package datadogexp

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/atlassian/gostatsd"
	stats "github.com/atlassian/gostatsd/pkg/statser"

	"github.com/ash2k/stager"
	"github.com/atlassian/gostatsd/pkg/statsd"
	"github.com/cenkalti/backoff"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	BackendName                  = "datadogexp"
	defaultUpstreamURL           = "https://app.datadoghq.com"
	defaultUserAgent             = "gostatsd"
	defaultMaxRequestElapsedTime = 15 * time.Second
	defaultClientTimeout         = 9 * time.Second
	defaultMetricsPerBatch       = 1000
	defaultNumWriters            = 40 // TODO: f(cores)
	defaultNumSerializers        = 40 // TODO: f(cores)
	defaultBufferIntervals       = 24 // assuming 10s interval, gives 240s / 4m

	defaultEnableHttp2 = false
)

// datadogClient represents an experimental Datadog client.
type datadogClient struct {
	logger         logrus.FieldLogger
	numWriters     int
	numSerializers int

	// metrics pathway
	metricsQueue    chan<- *batchMessage
	metricBuffer    *metricBuffer
	metricBatcher   *metricBatcher
	batchSerializer *batchSerializer
	bodyWriter      *bodyWriter

	// events pathway
	// ...
}

func (d *datadogClient) Run(ctx context.Context) {
	d.logger.Info("Starting")
	defer d.logger.Info("Terminating")
	stgr := stager.New()
	stage := stgr.NextStage()
	for i := 0; i < d.numWriters; i++ {
		stage.StartWithContext(d.bodyWriter.Run) // Network bound
	}
	stage = stgr.NextStage()
	for i := 0; i < d.numSerializers; i++ {
		stage.StartWithContext(d.batchSerializer.Run) // CPU bound
	}
	stage = stgr.NextStage()
	stage.StartWithContext(d.metricBatcher.Run) // Single instance
	stage = stgr.NextStage()
	stage.StartWithContext(d.metricBuffer.Run) // Single instance
	<-ctx.Done()
	stgr.Shutdown()
}

// SendMetricsAsync will send metrics asynchronously, invoking cb once the map is no longer used.
func (d *datadogClient) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, flushTime time.Time, cb gostatsd.SendCallback) {
	bm := &batchMessage{
		metrics:   metrics.Copy(), // metrics may not be accessed after this function returns
		flushTime: flushTime,
		done:      cb,
	}

	select {
	case <-ctx.Done():
	case d.metricsQueue <- bm:
	}
}

func (d *datadogClient) RunMetrics(ctx context.Context, statser stats.Statser) {
	statser = statser.WithTags(gostatsd.Tags{"backend:datadogexp"})

	flushed, unregister := statser.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushed:
		}
	}
}

// SendEvent sends an event to Datadog.
// TODO
func (d *datadogClient) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	/*
		return d.post(ctx, buffer, "/api/v1/events", "events", &event{
			Title:          e.Title,
			Text:           e.Text,
			DateHappened:   e.DateHappened,
			Hostname:       e.Hostname,
			AggregationKey: e.AggregationKey,
			SourceTypeName: e.SourceTypeName,
			Tags:           e.Tags,
			Priority:       e.Priority.StringWithEmptyDefault(),
			AlertType:      e.AlertType.StringWithEmptyDefault(),
		})
	*/
	return nil
}

// Name returns the name of the backend.
func (d *datadogClient) Name() string {
	return BackendName
}

// NewClientFromViper returns a new Datadog API client.
func NewClientFromViper(v *viper.Viper) (gostatsd.Backend, error) {
	dd := getSubViper(v, "datadogexp")
	dd.SetDefault("api-endpoint", defaultUpstreamURL)
	dd.SetDefault("user-agent", defaultUserAgent)
	dd.SetDefault("network", "tcp")
	dd.SetDefault("metrics-per-batch", defaultMetricsPerBatch)
	dd.SetDefault("num-writers", defaultNumWriters)
	dd.SetDefault("num-serializers", defaultNumSerializers)
	dd.SetDefault("buffer-intervals", defaultBufferIntervals)
	dd.SetDefault("compress-payload", true)
	dd.SetDefault("client-timeout", defaultClientTimeout)
	dd.SetDefault("max-request-elapsed-time", defaultMaxRequestElapsedTime)
	dd.SetDefault("enable-http2", defaultEnableHttp2)

	return NewClient(
		logrus.StandardLogger(),
		dd.GetString("api-endpoint"),
		dd.GetString("api-key"),
		dd.GetString("user-agent"),
		dd.GetString("network"),
		dd.GetInt("metrics-per-batch"),
		v.GetInt(statsd.ParamMaxWorkers), // Main viper - num of aggregators
		dd.GetInt("num-writers"),
		dd.GetInt("num-serializers"),
		dd.GetInt("buffer-intervals"),
		dd.GetBool("compress-payload"),
		dd.GetBool("enable-http2"),
		dd.GetDuration("client-timeout"),
		dd.GetDuration("max-request-elapsed-time"),
		v.GetDuration(statsd.ParamFlushInterval), // Main viper
		gostatsd.DisabledSubMetrics(v),
	)
}

// NewClient returns a new experimental Datadog API client.
func NewClient(
	logger logrus.FieldLogger,
	apiEndpoint, apiKey, userAgent, network string,
	metricsPerBatch, numAggregators, numWriters, numSerializers, bufIntervals int,
	compressPayload, enableHttp2 bool,
	clientTimeout, maxRequestElapsedTime, flushInterval time.Duration,
	disabled gostatsd.TimerSubtypes,
) (*datadogClient, error) {
	logger = logger.WithField("backend", BackendName) // TODO: Push this out

	if apiEndpoint == "" {
		return nil, fmt.Errorf("[%s] api-endpoint is required", BackendName)
	}
	if apiKey == "" {
		return nil, fmt.Errorf("[%s] api-key is required", BackendName)
	}
	if metricsPerBatch <= 0 {
		return nil, fmt.Errorf("[%s] metrics-per-batch must be positive", BackendName)
	}
	if clientTimeout <= 0 {
		return nil, fmt.Errorf("[%s] client-timeout must be positive", BackendName)
	}
	if maxRequestElapsedTime <= 0 {
		return nil, fmt.Errorf("[%s] max-request-elapsed-time must be positive", BackendName)
	}
	if numWriters <= 0 {
		return nil, fmt.Errorf("[%s] num-writers must be positive", BackendName)
	}
	if numSerializers <= 0 {
		return nil, fmt.Errorf("[%s] num-serializers must be positive", BackendName)
	}

	logger.WithFields(logrus.Fields{
		"apiEndpoint":           apiEndpoint,
		"userAgent":             userAgent,
		"network":               network,
		"metricsPerBatch":       metricsPerBatch,
		"numWriters":            numWriters,
		"numSerializers":        numSerializers,
		"compressPayload":       compressPayload,
		"enableHttp2":           enableHttp2,
		"clientTimeout":         clientTimeout,
		"maxRequestElapsedTime": maxRequestElapsedTime,
	}).Info("backend starting")

	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSHandshakeTimeout: 3 * time.Second,
		TLSClientConfig: &tls.Config{
			// Can't use SSLv3 because of POODLE and BEAST
			// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
			// Can't use TLSv1.1 because of RC4 cipher usage
			MinVersion: tls.VersionTLS12,
		},
		DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
			// replace the network with our own
			return dialer.DialContext(ctx, network, address)
		},
		MaxIdleConns:    50,
		IdleConnTimeout: 1 * time.Minute,
	}

	if !enableHttp2 {
		// A non-nil empty map used in TLSNextProto to disable HTTP/2 support in client.
		// https://golang.org/doc/go1.6#http2
		transport.TLSNextProto = map[string](func(string, *tls.Conn) http.RoundTripper){}
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   clientTimeout,
	}

	metricsQueue := make(chan *batchMessage, numAggregators)
	bufferRequested := make(chan struct{})
	bufferToBatcher := make(chan ddMetricMap)
	batcherToSerializer := make(chan ddTimeSeries)
	serializerToWriter := make(chan *renderedBody)

	metricBuffer := &metricBuffer{
		logger:           logger.WithField("component", "metricBuffer"),
		disabledSubTypes: &disabled,
		flushIntervalSec: flushInterval.Seconds(),
		capacity:         bufIntervals,
		metricsQueue:     metricsQueue,
		bufferRequested:  bufferRequested,
		bufferReady:      bufferToBatcher,
	}

	metricBatcher := &metricBatcher{
		logger:          logger.WithField("component", "metricBatcher"),
		interval:        flushInterval,
		metricsPerBatch: metricsPerBatch,
		requestData:     bufferRequested,
		receiveData:     bufferToBatcher,
		submitBatch:     batcherToSerializer,
	}

	batchSerializer := &batchSerializer{
		logger:       logger.WithField("component", "batchSerializer"),
		compress:     compressPayload,
		receiveBatch: batcherToSerializer,
		submitBody:   serializerToWriter,
		json: jsoniter.Config{
			EscapeHTML:  false,
			SortMapKeys: false,
		}.Froze(),
	}

	bodyWriter, err := newBodyWriter(
		logger.WithField("component", "bodyWriter"),
		userAgent,
		apiEndpoint,
		apiKey,
		httpClient,
		func() backoff.BackOff {
			bo := backoff.NewExponentialBackOff()
			bo.MaxElapsedTime = maxRequestElapsedTime
			return bo
		},
		serializerToWriter,
	)

	if err != nil {
		return nil, fmt.Errorf("[%s] Unable to create bodyWriter: %v", BackendName, err)
	}

	return &datadogClient{
		logger:         logger.WithField("component", "main"),
		numWriters:     numWriters,
		numSerializers: numSerializers,

		metricsQueue:    metricsQueue,
		metricBuffer:    metricBuffer,
		metricBatcher:   metricBatcher,
		batchSerializer: batchSerializer,
		bodyWriter:      bodyWriter,
	}, nil
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	return n
}
