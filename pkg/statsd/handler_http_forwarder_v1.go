package statsd

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pb"
	"github.com/atlassian/gostatsd/pkg/stats"

	"github.com/ash2k/stager/wait"
	"github.com/cenkalti/backoff"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/tilinna/clock"
)

const (
	defaultConsolidatorFlushInterval = 1 * time.Second
	defaultClientTimeout             = 10 * time.Second
	defaultCompress                  = true
	defaultEnableHttp2               = false
	defaultEndpoint                  = ""
	defaultMaxRequestElapsedTime     = 30 * time.Second
	defaultMaxRequests               = 1000
	defaultNetwork                   = "tcp"
	defaultMetricsPerBatch           = 1000
)

// HttpForwarderHandlerV1 is a PipelineHandler which sends metrics to another gostatsd instance
type HttpForwarderHandlerV1 struct {
	postId          uint64 // atomic - used for an id in logs
	messagesInvalid uint64 // atomic - messages which failed to be created
	messagesCreated uint64 // atomic - messages which were created
	messagesSent    uint64 // atomic - messages successfully sent
	messagesRetried uint64 // atomic - retries (first send is not a retry, final failure is not a retry)
	messagesDropped uint64 // atomic - final failure

	logger                logrus.FieldLogger
	apiEndpoint           string
	maxRequestElapsedTime time.Duration
	metricsSem            chan struct{}
	client                http.Client
	compress              bool
	consolidator          gostatsd.RawMetricHandler
	consolidatedMetrics   <-chan []*gostatsd.Metric
	eventWg               sync.WaitGroup
}

// NewHttpForwarderHandlerV1FromViper returns a new http API client.
func NewHttpForwarderHandlerV1FromViper(logger logrus.FieldLogger, v *viper.Viper) (*HttpForwarderHandlerV1, error) {
	subViper := getSubViper(v, "http-transport")
	subViper.SetDefault("client-timeout", defaultClientTimeout)
	subViper.SetDefault("compress", defaultCompress)
	subViper.SetDefault("enable-http2", defaultEnableHttp2)
	subViper.SetDefault("endpoint", defaultEndpoint)
	subViper.SetDefault("max-requests", defaultMaxRequests)
	subViper.SetDefault("max-request-elapsed-time", defaultMaxRequestElapsedTime)
	subViper.SetDefault("metrics-per-batch", defaultMetricsPerBatch)
	subViper.SetDefault("flush-interval", defaultConsolidatorFlushInterval)
	subViper.SetDefault("network", defaultNetwork)

	return NewHttpForwarderHandlerV1(
		logger,
		subViper.GetString("api-endpoint"),
		subViper.GetString("network"),
		subViper.GetInt("metrics-per-batch"),
		subViper.GetInt("max-requests"),
		subViper.GetBool("compress"),
		subViper.GetBool("enable-http2"),
		subViper.GetDuration("client-timeout"),
		subViper.GetDuration("max-request-elapsed-time"),
		subViper.GetDuration("flush-interval"),
	)
}

// NewHttpForwarderHandlerV1 returns a new handler which dispatches metrics over http to another gostatsd server.
func NewHttpForwarderHandlerV1(logger logrus.FieldLogger, apiEndpoint, network string, metricsPerBatch, maxRequests int, compress, enableHttp2 bool, clientTimeout, maxRequestElapsedTime time.Duration, flushInterval time.Duration) (*HttpForwarderHandlerV1, error) {
	if apiEndpoint == "" {
		return nil, fmt.Errorf("api-endpoint is required")
	}
	if metricsPerBatch <= 0 {
		return nil, fmt.Errorf("metrics-per-batch must be positive")
	}
	if maxRequests <= 0 {
		return nil, fmt.Errorf("max-requests must be positive")
	}
	if clientTimeout <= 0 {
		return nil, fmt.Errorf("client-timeout must be positive")
	}
	if maxRequestElapsedTime <= 0 {
		return nil, fmt.Errorf("max-request-elapsed-time must be positive")
	}
	if flushInterval <= 0 {
		return nil, fmt.Errorf("flush-interval must be positive")
	}

	logger.WithFields(logrus.Fields{
		"api-endpoint":             apiEndpoint,
		"client-timeout":           clientTimeout,
		"compress":                 compress,
		"enable-http2":             enableHttp2,
		"max-request-elapsed-time": maxRequestElapsedTime,
		"max-requests":             maxRequests,
		"metrics-per-batch":        metricsPerBatch,
		"network":                  network,
		"flush-interval":           flushInterval,
	}).Info("created HttpForwarderHandler")

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

	metricsSem := make(chan struct{}, maxRequests)
	for i := 0; i < maxRequests; i++ {
		metricsSem <- struct{}{}
	}

	ch := make(chan []*gostatsd.Metric)

	return &HttpForwarderHandlerV1{
		logger:                logger.WithField("component", "http-forwarder-handler"),
		apiEndpoint:           apiEndpoint,
		maxRequestElapsedTime: maxRequestElapsedTime,
		metricsSem:            metricsSem,
		compress:              compress,
		consolidator:          gostatsd.NewMetricBatcher(metricsPerBatch, flushInterval, ch),
		consolidatedMetrics:   ch,
		client: http.Client{
			Transport: transport,
			Timeout:   clientTimeout,
		},
	}, nil
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	return n
}

func (hfh *HttpForwarderHandlerV1) EstimatedTags() int {
	return 0
}

func (hfh *HttpForwarderHandlerV1) DispatchMetric(ctx context.Context, m *gostatsd.Metric) {
	hfh.consolidator.DispatchMetric(ctx, m)
}

func (hfh *HttpForwarderHandlerV1) RunMetrics(ctx context.Context) {
	statser := stats.FromContext(ctx)

	notify, cancel := statser.RegisterFlush()
	defer cancel()

	for {
		select {
		case <-notify:
			hfh.emitMetrics(statser)
		case <-ctx.Done():
			return
		}
	}
}

func (hfh *HttpForwarderHandlerV1) emitMetrics(statser stats.Statser) {
	messagesInvalid := atomic.SwapUint64(&hfh.messagesInvalid, 0)
	messagesCreated := atomic.SwapUint64(&hfh.messagesCreated, 0)
	messagesSent := atomic.SwapUint64(&hfh.messagesSent, 0)
	messagesRetried := atomic.SwapUint64(&hfh.messagesRetried, 0)
	messagesDropped := atomic.SwapUint64(&hfh.messagesDropped, 0)

	statser.Count("http.forwarder.invalid", float64(messagesInvalid), nil)
	statser.Count("http.forwarder.created", float64(messagesCreated), nil)
	statser.Count("http.forwarder.sent", float64(messagesSent), nil)
	statser.Count("http.forwarder.retried", float64(messagesRetried), nil)
	statser.Count("http.forwarder.dropped", float64(messagesDropped), nil)
}

func (hfh *HttpForwarderHandlerV1) Run(ctx context.Context) {
	if r, ok := hfh.consolidator.(gostatsd.Runner); ok {
		var wg wait.Group
		defer wg.Wait()
		wg.StartWithContext(ctx, r.Run)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case metrics := <-hfh.consolidatedMetrics:
			if !hfh.acquireSem(ctx) {
				return
			}
			postId := atomic.AddUint64(&hfh.postId, 1) - 1
			go func(postId uint64) {
				hfh.postMetrics(ctx, metrics, postId)
				hfh.releaseSem()
			}(postId)
		}
	}
}

func (hfh *HttpForwarderHandlerV1) acquireSem(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-hfh.metricsSem:
		return true
	}
}

func (hfh *HttpForwarderHandlerV1) releaseSem() {
	hfh.metricsSem <- struct{}{} // will never block
}

func translateToProtobuf(metrics []*gostatsd.Metric) []*pb.RawMetricV1 {
	pbMetrics := make([]*pb.RawMetricV1, 0, len(metrics))

	for _, metric := range metrics {
		pbm := &pb.RawMetricV1{
			Name:     metric.Name,
			Hostname: metric.Hostname,
			Tags:     metric.Tags,
		}
		switch metric.Type {
		case gostatsd.COUNTER:
			pbm.M = &pb.RawMetricV1_Counter{
				Counter: &pb.RawCounterV1{
					Value: metric.Value / metric.Rate,
				},
			}
		case gostatsd.GAUGE:
			pbm.M = &pb.RawMetricV1_Gauge{
				Gauge: &pb.RawGaugeV1{
					Value: metric.Value,
				},
			}
		case gostatsd.SET:
			pbm.M = &pb.RawMetricV1_Set{
				Set: &pb.RawSetV1{
					Value: metric.StringValue,
				},
			}
		case gostatsd.TIMER:
			pbm.M = &pb.RawMetricV1_Timer{
				Timer: &pb.RawTimerV1{
					Value: metric.Value,
					Rate:  metric.Rate,
				},
			}
		default:
			// In theory this only happens in tests, and it should be a panic, but in the event
			// that it somehow occurs in operation, we want to keep running as best we can.
			continue
		}
		pbMetrics = append(pbMetrics, pbm)
	}
	return pbMetrics
}

func (hfh *HttpForwarderHandlerV1) postMetrics(ctx context.Context, metrics []*gostatsd.Metric, batchId uint64) {
	message := &pb.RawMessageV1{
		RawMetrics: translateToProtobuf(metrics),
	}
	if len(message.RawMetrics) == 0 {
		return
	}
	hfh.post(ctx, message, batchId, "metrics", "/v1/raw")
}

func (hfh *HttpForwarderHandlerV1) post(ctx context.Context, message proto.Message, id uint64, endpointType, endpoint string) {
	logger := hfh.logger.WithFields(logrus.Fields{
		"id":   id,
		"type": endpointType,
	})

	post, err := hfh.constructPost(ctx, logger, hfh.apiEndpoint+endpoint, message)
	if err != nil {
		atomic.AddUint64(&hfh.messagesInvalid, 1)
		logger.WithError(err).Error("failed to create request")
		return
	} else {
		atomic.AddUint64(&hfh.messagesCreated, 1)
	}

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = hfh.maxRequestElapsedTime

	for {
		if err = post(); err == nil {
			atomic.AddUint64(&hfh.messagesSent, 1)
			return
		}

		next := b.NextBackOff()
		if next == backoff.Stop {
			atomic.AddUint64(&hfh.messagesDropped, 1)
			logger.WithError(err).Error("failed to send, giving up")
			return
		}

		atomic.AddUint64(&hfh.messagesRetried, 1)
		logger.WithError(err).Warn("failed to send, retrying")

		timer := clock.NewTimer(ctx, next)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (hfh *HttpForwarderHandlerV1) serialize(message proto.Message) ([]byte, error) {
	buf, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// debug rendering
/*
func (hh *HttpForwarderHandlerV1) serializeText(message proto.Message) ([]byte, error) {
	buf := &bytes.Buffer{}
	err := proto.MarshalText(buf, message)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
*/

func (hfh *HttpForwarderHandlerV1) serializeAndCompress(message proto.Message) ([]byte, error) {
	raw, err := hfh.serialize(message)
	if err != nil {
		return nil, err
	}

	buf := &bytes.Buffer{}
	compressor, err := zlib.NewWriterLevel(buf, zlib.BestCompression)
	if err != nil {
		return nil, err
	}

	compressor.Write(raw)
	err = compressor.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (hfh *HttpForwarderHandlerV1) constructPost(ctx context.Context, logger logrus.FieldLogger, path string, message proto.Message) (func() error /*doPost*/, error) {
	var body []byte
	var err error
	var encoding string

	if hfh.compress {
		body, err = hfh.serializeAndCompress(message)
		encoding = "deflate"
	} else {
		body, err = hfh.serialize(message)
		encoding = "identity"
	}

	if err != nil {
		return nil, err
	}

	return func() error {
		headers := map[string]string{
			"Content-Type":     "application/x-protobuf",
			"Content-Encoding": encoding,
			"User-Agent":       "gostatsd (http forwarder)",
		}
		req, err := http.NewRequest("POST", path, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("unable to create http.Request: %v", err)
		}
		req = req.WithContext(ctx)
		for header, v := range headers {
			req.Header.Set(header, v)
		}
		resp, err := hfh.client.Do(req)
		if err != nil {
			return fmt.Errorf("error POSTing: %v", err)
		}
		defer func() {
			_, _ = io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			bodyStart, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
			logger.WithFields(logrus.Fields{
				"status": resp.StatusCode,
				"body":   string(bodyStart),
			}).Info("failed request")
			return fmt.Errorf("received bad status code %d", resp.StatusCode)
		}
		return nil
	}, nil
}

///////// Event processing

// Events are handled individually, because the context matters. If they're buffered through the consolidator, they'll
// be processed on a goroutine with a context which will be closed during shutdown.  Events should be rare enough that
// this isn't an issue.

func (hfh *HttpForwarderHandlerV1) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	hfh.eventWg.Add(1)
	go hfh.dispatchEvent(ctx, e)
}

func (hfh *HttpForwarderHandlerV1) dispatchEvent(ctx context.Context, e *gostatsd.Event) {
	postId := atomic.AddUint64(&hfh.postId, 1) - 1

	message := &pb.EventV1{
		Title:          e.Title,
		Text:           e.Text,
		DateHappened:   e.DateHappened,
		Hostname:       e.Hostname,
		AggregationKey: e.AggregationKey,
		SourceTypeName: e.SourceTypeName,
		Tags:           e.Tags,
		SourceIP:       string(e.SourceIP),
	}

	switch e.Priority {
	case gostatsd.PriNormal:
		message.Priority = pb.EventV1_Normal
	case gostatsd.PriLow:
		message.Priority = pb.EventV1_Low
	}

	switch e.AlertType {
	case gostatsd.AlertInfo:
		message.Type = pb.EventV1_Info
	case gostatsd.AlertWarning:
		message.Type = pb.EventV1_Warning
	case gostatsd.AlertError:
		message.Type = pb.EventV1_Error
	case gostatsd.AlertSuccess:
		message.Type = pb.EventV1_Success
	}

	hfh.post(ctx, message, postId, "event", "/v1/event")

	defer hfh.eventWg.Done()
}

func (hfh *HttpForwarderHandlerV1) WaitForEvents() {
	hfh.eventWg.Wait()
}
