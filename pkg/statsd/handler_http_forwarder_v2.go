package statsd

import (
	"bytes"
	"compress/zlib"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ash2k/stager/wait"
	"github.com/cenkalti/backoff"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/tilinna/clock"
	"google.golang.org/protobuf/proto"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/flush"
	"github.com/atlassian/gostatsd/internal/util"
	"github.com/atlassian/gostatsd/pb"
	"github.com/atlassian/gostatsd/pkg/healthcheck"
	"github.com/atlassian/gostatsd/pkg/stats"
	"github.com/atlassian/gostatsd/pkg/transport"
)

const (
	defaultConsolidatorFlushInterval = 1 * time.Second
	defaultCompress                  = true
	defaultApiEndpoint               = ""
	defaultMaxRequestElapsedTime     = 30 * time.Second
	defaultMaxRequests               = 1000
	defaultConcurrentMerge           = 1
	defaultTransport                 = "default"
)

// HttpForwarderHandlerV2 is a PipelineHandler which sends metrics to another gostatsd instance
type HttpForwarderHandlerV2 struct {
	postId             uint64 // atomic - used for an id in logs
	messagesInvalid    uint64 // atomic - messages which failed to be created
	messagesCreated    uint64 // atomic - messages which were created
	messagesSent       uint64 // atomic - messages successfully sent
	messagesRetried    uint64 // atomic - retries (first send is not a retry, final failure is not a retry)
	messagesDropped    uint64 // atomic - final failure
	lastSuccessfulSend atomic.Int64

	logger                logrus.FieldLogger
	apiEndpoint           string
	maxRequestElapsedTime time.Duration
	metricsSem            chan struct{}
	metricsMergingSem     chan struct{}
	client                *http.Client
	eventWg               sync.WaitGroup
	compress              bool
	headers               map[string]string
	dynHeaderNames        []string

	done                chan struct{}
	consolidator        *gostatsd.MetricConsolidator
	consolidatedMetrics chan []*gostatsd.MetricMap

	flushCoordinator flush.Coordinator
}

var (
	_ healthcheck.DeepCheckProvider = &HttpForwarderHandlerV2{}
)

// NewHttpForwarderHandlerV2FromViper returns a new http API client.
func NewHttpForwarderHandlerV2FromViper(logger logrus.FieldLogger, v *viper.Viper, pool *transport.TransportPool, fc flush.Coordinator) (*HttpForwarderHandlerV2, error) {
	subViper := util.GetSubViper(v, "http-transport")
	subViper.SetDefault("transport", defaultTransport)
	subViper.SetDefault("compress", defaultCompress)
	subViper.SetDefault("api-endpoint", defaultApiEndpoint)
	subViper.SetDefault("max-requests", defaultMaxRequests)
	subViper.SetDefault("max-request-elapsed-time", defaultMaxRequestElapsedTime)
	subViper.SetDefault("consolidator-slots", v.GetInt(gostatsd.ParamMaxParsers))
	subViper.SetDefault("flush-interval", defaultConsolidatorFlushInterval)
	subViper.SetDefault("concurrent-merge", defaultConcurrentMerge)

	return NewHttpForwarderHandlerV2(
		logger,
		subViper.GetString("transport"),
		subViper.GetString("api-endpoint"),
		subViper.GetInt("consolidator-slots"),
		subViper.GetInt("max-requests"),
		subViper.GetInt("concurrent-merge"),
		subViper.GetBool("compress"),
		subViper.GetDuration("max-request-elapsed-time"),
		subViper.GetDuration("flush-interval"),
		subViper.GetStringMapString("custom-headers"),
		subViper.GetStringSlice("dynamic-headers"),
		pool,
		fc,
	)
}

// NewHttpForwarderHandlerV2 returns a new handler which dispatches metrics over http to another gostatsd server.
func NewHttpForwarderHandlerV2(
	logger logrus.FieldLogger,
	transport,
	apiEndpoint string,
	consolidatorSlots,
	maxRequests int,
	concurrentMerge int,
	compress bool,
	maxRequestElapsedTime time.Duration,
	flushInterval time.Duration,
	xheaders map[string]string,
	dynHeaderNames []string,
	pool *transport.TransportPool,
	fc flush.Coordinator,
) (*HttpForwarderHandlerV2, error) {
	if apiEndpoint == "" {
		return nil, fmt.Errorf("api-endpoint is required")
	}
	if consolidatorSlots <= 0 {
		return nil, fmt.Errorf("consolidator-slots must be positive")
	}
	if maxRequests <= 0 {
		return nil, fmt.Errorf("max-requests must be positive")
	}
	if maxRequestElapsedTime <= 0 && maxRequestElapsedTime != -1 {
		return nil, fmt.Errorf("max-request-elapsed-time must be positive")
	}
	if flushInterval <= 0 {
		return nil, fmt.Errorf("flush-interval must be positive")
	}
	if concurrentMerge <= 0 {
		return nil, fmt.Errorf("concurrent-merge must be positive")
	}

	httpClient, err := pool.Get(transport)
	if err != nil {
		logger.WithError(err).Error("failed to create http client")
		return nil, err
	}

	logger.WithFields(logrus.Fields{
		"api-endpoint":             apiEndpoint,
		"compress":                 compress,
		"max-request-elapsed-time": maxRequestElapsedTime,
		"max-requests":             maxRequests,
		"consolidator-slots":       consolidatorSlots,
		"flush-interval":           flushInterval,
	}).Info("created HttpForwarderHandler")

	// Default set of headers used for the forwarder
	// Once these values are set, modifying the map is illadvised
	// due to the fact that map is just a reference to memory.
	headers := map[string]string{
		"Content-Type": "application/x-protobuf",
		"User-Agent":   "gostatsd (http forwarder)",
	}

	// Adding extra headers to the default block of headers to emit.
	for k, v := range xheaders {
		k = http.CanonicalHeaderKey(k)
		headers[k] = v
	}

	dynHeaderNamesWithColon := make([]string, 0)
	for _, name := range dynHeaderNames {
		if name == "" {
			continue
		}
		if _, ok := xheaders[name]; !ok {
			dynHeaderNamesWithColon = append(dynHeaderNamesWithColon, name+":")
		} else {
			logger.WithField("header-name", name).Warn("static and dynamic header defined, using static")
		}
	}

	metricsSem := make(chan struct{}, maxRequests)
	for i := 0; i < maxRequests; i++ {
		metricsSem <- struct{}{}
	}

	metricsMergingSem := make(chan struct{}, concurrentMerge)
	for i := 0; i < concurrentMerge; i++ {
		metricsMergingSem <- struct{}{}
	}

	ch := make(chan []*gostatsd.MetricMap)
	consolidator := gostatsd.NewMetricConsolidator(consolidatorSlots, false, flushInterval, ch)

	if fc != nil {
		fc.RegisterFlushable(consolidator)
	}

	return &HttpForwarderHandlerV2{
		logger:                logger.WithField("component", "http-forwarder-handler-v2"),
		apiEndpoint:           apiEndpoint,
		maxRequestElapsedTime: maxRequestElapsedTime,
		metricsSem:            metricsSem,
		metricsMergingSem:     metricsMergingSem,
		compress:              compress,
		consolidator:          consolidator,
		consolidatedMetrics:   ch,
		client:                httpClient.Client,
		headers:               headers,
		dynHeaderNames:        dynHeaderNamesWithColon,
		done:                  make(chan struct{}),
		flushCoordinator:      fc,
	}, nil
}

func (hfh *HttpForwarderHandlerV2) DeepChecks() []healthcheck.HealthcheckFunc {
	return []healthcheck.HealthcheckFunc{
		hfh.dcSentRecently,
	}
}

func (hfh *HttpForwarderHandlerV2) dcSentRecently() (string, healthcheck.HealthyStatus) {
	if lastSend := hfh.lastSuccessfulSend.Load(); lastSend == 0 {
		return "sentRecently: never sent", healthcheck.Unhealthy
	} else {
		return fmt.Sprintf("sentRecently: lastSend=%s", time.Unix(0, lastSend).String()), healthcheck.Healthy
	}
}

func (hfh *HttpForwarderHandlerV2) EstimatedTags() int {
	return 0
}

// DispatchMetricMap dispatches a metric map to the MetricConsolidator
func (hfh *HttpForwarderHandlerV2) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
	select {
	case <-ctx.Done():
		hfh.logger.WithError(ctx.Err()).Debug("Context is cancelled")
	case <-hfh.done:
		hfh.logger.Debug("Forwarder has shutdown and unable to accept metrics")
	default:
		hfh.consolidator.ReceiveMetricMap(mm)
	}
}

func (hfh *HttpForwarderHandlerV2) RunMetricsContext(ctx context.Context) {
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

func (hfh *HttpForwarderHandlerV2) emitMetrics(statser stats.Statser) {
	statser.Report("http.forwarder.invalid", &hfh.messagesInvalid, nil)
	statser.Report("http.forwarder.created", &hfh.messagesCreated, nil)
	statser.Report("http.forwarder.sent", &hfh.messagesSent, nil)
	statser.Report("http.forwarder.retried", &hfh.messagesRetried, nil)
	statser.Report("http.forwarder.dropped", &hfh.messagesDropped, nil)
}

// sendNop sends an empty metric map downstream.  It's used to "prime the pump" for the deepcheck.
func (hfh *HttpForwarderHandlerV2) sendNop(ctx context.Context) {
	hfh.postMetrics(ctx, gostatsd.NewMetricMap(false), "", 0)
}

func (hfh *HttpForwarderHandlerV2) Run(ctx context.Context) {
	var wg wait.Group
	wg.StartWithContext(ctx, hfh.sendNop)
	wg.Start(func() {
		for metricMaps := range hfh.consolidatedMetrics {
			hfh.acquireMergingSem()
			metricMaps := metricMaps
			go func() {
				mergedMetricMap := gostatsd.MergeMaps(metricMaps)
				mms := mergedMetricMap.SplitByTags(hfh.dynHeaderNames)
				hfh.releaseMergingSem()

				for dynHeaderTags, mm := range mms {
					if mm.IsEmpty() {
						hfh.notifyFlush()
						continue
					}
					hfh.acquireSem()
					postId := atomic.AddUint64(&hfh.postId, 1) - 1
					go func(postId uint64, metricMap *gostatsd.MetricMap, dynHeaderTags string) {
						hfh.postMetrics(context.Background(), metricMap, dynHeaderTags, postId)
						hfh.notifyFlush()
						hfh.releaseSem()
					}(postId, mm, dynHeaderTags)
				}
			}()
		}
		for i := 0; i < cap(hfh.metricsSem); i++ {
			hfh.acquireSem()
		}
		for i := 0; i < cap(hfh.metricsMergingSem); i++ {
			hfh.acquireMergingSem()
		}
	})

	wg.StartWithContext(ctx, func(ctx context.Context) {
		if hfh.flushCoordinator == nil {
			hfh.consolidator.Run(ctx)
		} else {
			<-ctx.Done()
		}

		hfh.Close()
	})

	wg.Wait()
	hfh.logger.Debug("Shutdown http forwarder")
}

func (hfh *HttpForwarderHandlerV2) Close() {
	close(hfh.done)
	close(hfh.consolidatedMetrics)
}

func (hfh *HttpForwarderHandlerV2) acquireSem() {
	<-hfh.metricsSem // will potentially block
}

func (hfh *HttpForwarderHandlerV2) releaseSem() {
	hfh.metricsSem <- struct{}{} // will never block
}

func (hfh *HttpForwarderHandlerV2) acquireMergingSem() {
	<-hfh.metricsMergingSem
}

func (hfh *HttpForwarderHandlerV2) releaseMergingSem() {
	hfh.metricsMergingSem <- struct{}{}
}

func (hfh *HttpForwarderHandlerV2) notifyFlush() {
	if hfh.flushCoordinator != nil {
		hfh.flushCoordinator.NotifyFlush()
	}
}

func translateToProtobufV2(metricMap *gostatsd.MetricMap) *pb.RawMessageV2 {
	var pbMetricMap pb.RawMessageV2

	pbMetricMap.Gauges = map[string]*pb.GaugeTagV2{}
	for metricName, m := range metricMap.Gauges {
		pbMetricMap.Gauges[metricName] = &pb.GaugeTagV2{TagMap: map[string]*pb.RawGaugeV2{}}
		for tagsKey, metric := range m {
			pbMetricMap.Gauges[metricName].TagMap[tagsKey] = &pb.RawGaugeV2{
				Tags:     metric.Tags,
				Hostname: string(metric.Source),
				Value:    metric.Value,
			}
		}
	}

	pbMetricMap.Counters = map[string]*pb.CounterTagV2{}
	for metricName, m := range metricMap.Counters {
		pbMetricMap.Counters[metricName] = &pb.CounterTagV2{TagMap: map[string]*pb.RawCounterV2{}}
		for tagsKey, metric := range m {
			pbMetricMap.Counters[metricName].TagMap[tagsKey] = &pb.RawCounterV2{
				Tags:     metric.Tags,
				Hostname: string(metric.Source),
				Value:    metric.Value,
			}
		}
	}

	pbMetricMap.Sets = map[string]*pb.SetTagV2{}
	for metricName, m := range metricMap.Sets {
		pbMetricMap.Sets[metricName] = &pb.SetTagV2{TagMap: map[string]*pb.RawSetV2{}}
		for tagsKey, metric := range m {
			var values []string
			for key := range metric.Values {
				values = append(values, key)
			}
			pbMetricMap.Sets[metricName].TagMap[tagsKey] = &pb.RawSetV2{
				Tags:     metric.Tags,
				Hostname: string(metric.Source),
				Values:   values,
			}
		}
	}

	pbMetricMap.Timers = map[string]*pb.TimerTagV2{}
	for metricName, m := range metricMap.Timers {
		pbMetricMap.Timers[metricName] = &pb.TimerTagV2{TagMap: map[string]*pb.RawTimerV2{}}
		for tagsKey, metric := range m {
			pbMetricMap.Timers[metricName].TagMap[tagsKey] = &pb.RawTimerV2{
				Tags:        metric.Tags,
				Hostname:    string(metric.Source),
				SampleCount: metric.SampledCount,
				Values:      metric.Values,
			}
		}
	}

	return &pbMetricMap
}

func (hfh *HttpForwarderHandlerV2) postMetrics(ctx context.Context, metricMap *gostatsd.MetricMap, dynHeaderTags string, batchId uint64) {
	message := translateToProtobufV2(metricMap)
	hfh.post(ctx, message, dynHeaderTags, batchId, "metrics", "/v2/raw")
}

func (hfh *HttpForwarderHandlerV2) post(ctx context.Context, message proto.Message, dynHeaderTags string, id uint64, endpointType, endpoint string) {
	logger := hfh.logger.WithFields(logrus.Fields{
		"id":   id,
		"type": endpointType,
	})

	post, err := hfh.constructPost(ctx, logger, hfh.apiEndpoint+endpoint, message, dynHeaderTags)
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
			hfh.lastSuccessfulSend.Store(clock.Now(ctx).UnixNano())
			return
		}

		next := b.NextBackOff()
		if next == backoff.Stop {
			atomic.AddUint64(&hfh.messagesDropped, 1)
			logger.WithError(err).Info("failed to send, giving up")
			return
		}

		atomic.AddUint64(&hfh.messagesRetried, 1)

		timer := clock.NewTimer(ctx, next)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// debug rendering
/*
func (hh *HttpForwarderHandlerV2) serializeText(message proto.Message) ([]byte, error) {
       buf := &bytes.Buffer{}
       err := proto.MarshalText(buf, message)
       if err != nil {
               return nil, err
       }
       return buf.Bytes(), nil
}
*/

func (hfh *HttpForwarderHandlerV2) serialize(message proto.Message) ([]byte, error) {
	buf, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (hfh *HttpForwarderHandlerV2) serializeAndCompress(message proto.Message) ([]byte, error) {
	raw, err := hfh.serialize(message)
	if err != nil {
		return nil, err
	}

	buf := &bytes.Buffer{}
	compressor, err := zlib.NewWriterLevel(buf, zlib.BestCompression)
	if err != nil {
		return nil, err
	}

	_, _ = compressor.Write(raw) // error is propagated through Close
	err = compressor.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (hfh *HttpForwarderHandlerV2) constructPost(ctx context.Context, logger logrus.FieldLogger, path string, message proto.Message, dynHeaderTags string) (func() error /*doPost*/, error) {
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
		req, err := http.NewRequest("POST", path, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("unable to create http.Request: %v", err)
		}
		req = req.WithContext(ctx)
		for _, tv := range strings.Split(dynHeaderTags, ",") {
			vs := strings.SplitN(tv, ":", 2)
			if len(vs) > 1 {
				req.Header.Set(strings.ReplaceAll(vs[0], "_", "-"), vs[1])
			}
		}
		for header, v := range hfh.headers {
			req.Header.Set(header, v)
		}
		req.Header.Set("Content-Encoding", encoding)
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

func (hfh *HttpForwarderHandlerV2) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	hfh.eventWg.Add(1)
	go hfh.dispatchEvent(ctx, e)
}

func (hfh *HttpForwarderHandlerV2) dispatchEvent(ctx context.Context, e *gostatsd.Event) {
	postId := atomic.AddUint64(&hfh.postId, 1) - 1

	message := &pb.EventV2{
		Title:          e.Title,
		Text:           e.Text,
		DateHappened:   e.DateHappened,
		Hostname:       string(e.Source),
		AggregationKey: e.AggregationKey,
		SourceTypeName: e.SourceTypeName,
		Tags:           e.Tags,
		SourceIP:       string(e.Source),
	}

	switch e.Priority {
	case gostatsd.PriNormal:
		message.Priority = pb.EventV2_Normal
	case gostatsd.PriLow:
		message.Priority = pb.EventV2_Low
	}

	switch e.AlertType {
	case gostatsd.AlertInfo:
		message.Type = pb.EventV2_Info
	case gostatsd.AlertWarning:
		message.Type = pb.EventV2_Warning
	case gostatsd.AlertError:
		message.Type = pb.EventV2_Error
	case gostatsd.AlertSuccess:
		message.Type = pb.EventV2_Success
	}

	hfh.post(ctx, message, "", postId, "event", "/v2/event")

	defer hfh.eventWg.Done()
}

func (hfh *HttpForwarderHandlerV2) WaitForEvents() {
	hfh.eventWg.Wait()
}
