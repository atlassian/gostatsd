package statsd

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pb"
	"github.com/atlassian/gostatsd/pkg/httpclient"

	"github.com/ash2k/stager/wait"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	defaultConsolidatorFlushInterval = 1 * time.Second
	defaultClientTimeout             = 10 * time.Second
	defaultCompress                  = true
	defaultEnableHttp2               = false
	defaultApiEndpoint               = ""
	defaultMaxRequestElapsedTime     = 30 * time.Second
	defaultMaxRequests               = 1000
	defaultNetwork                   = "tcp"
)

// HttpForwarderHandlerV2 is a PipelineHandler which sends metrics to another gostatsd instance
type HttpForwarderHandlerV2 struct {
	logger              logrus.FieldLogger
	apiEndpoint         string
	httpClient          *httpclient.HttpClient
	consolidator        *gostatsd.MetricConsolidator
	consolidatedMetrics <-chan []*gostatsd.MetricMap
	eventWg             sync.WaitGroup
}

// NewHttpForwarderHandlerV2FromViper returns a new http API client.
func NewHttpForwarderHandlerV2FromViper(logger logrus.FieldLogger, v *viper.Viper) (*HttpForwarderHandlerV2, error) {
	subViper := getSubViper(v, "http-transport")
	subViper.SetDefault("client-timeout", defaultClientTimeout)
	subViper.SetDefault("compress", defaultCompress)
	subViper.SetDefault("enable-http2", defaultEnableHttp2)
	subViper.SetDefault("api-endpoint", defaultApiEndpoint)
	subViper.SetDefault("max-requests", defaultMaxRequests)
	subViper.SetDefault("max-request-elapsed-time", defaultMaxRequestElapsedTime)
	subViper.SetDefault("consolidator-slots", v.GetInt(ParamMaxParsers))
	subViper.SetDefault("flush-interval", defaultConsolidatorFlushInterval)
	subViper.SetDefault("network", defaultNetwork)

	return NewHttpForwarderHandlerV2(
		logger,
		subViper.GetString("api-endpoint"),
		subViper.GetString("network"),
		subViper.GetInt("consolidator-slots"),
		subViper.GetInt("max-requests"),
		subViper.GetBool("compress"),
		subViper.GetBool("enable-http2"),
		subViper.GetDuration("client-timeout"),
		subViper.GetDuration("max-request-elapsed-time"),
		subViper.GetDuration("flush-interval"),
	)
}

// NewHttpForwarderHandlerV2 returns a new handler which dispatches metrics over http to another gostatsd server.
func NewHttpForwarderHandlerV2(logger logrus.FieldLogger, apiEndpoint, network string, consolidatorSlots, maxRequests int, compress, enableHttp2 bool, clientTimeout, maxRequestElapsedTime time.Duration, flushInterval time.Duration) (*HttpForwarderHandlerV2, error) {
	if apiEndpoint == "" {
		return nil, fmt.Errorf("api-endpoint is required")
	}
	if consolidatorSlots <= 0 {
		return nil, fmt.Errorf("consolidator-slots must be positive")
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
		"consolidator-slots":       consolidatorSlots,
		"network":                  network,
		"flush-interval":           flushInterval,
	}).Info("created HttpForwarderHandler")

	ch := make(chan []*gostatsd.MetricMap)

	hc, err := httpclient.NewHttpClient(logger, network, maxRequests, compress, enableHttp2, clientTimeout, maxRequestElapsedTime)
	if err != nil {
		return nil, err
	}

	return &HttpForwarderHandlerV2{
		logger:              logger.WithField("component", "http-forwarder-handler-v2"),
		apiEndpoint:         apiEndpoint,
		httpClient:          hc,
		consolidator:        gostatsd.NewMetricConsolidator(consolidatorSlots, false, flushInterval, ch),
		consolidatedMetrics: ch,
	}, nil
}

func (hfh *HttpForwarderHandlerV2) EstimatedTags() int {
	return 0
}

func (hfh *HttpForwarderHandlerV2) DispatchMetrics(ctx context.Context, metrics []*gostatsd.Metric) {
	hfh.consolidator.ReceiveMetrics(metrics)
}

// DispatchMetricMap re-dispatches a metric map through HttpForwarderHandlerV2.DispatchMetrics
func (hfh *HttpForwarderHandlerV2) DispatchMetricMap(ctx context.Context, mm *gostatsd.MetricMap) {
	hfh.consolidator.ReceiveMetricMap(mm)
}

func (hfh *HttpForwarderHandlerV2) Run(ctx context.Context) {
	var wg wait.Group
	defer wg.Wait()
	wg.StartWithContext(ctx, hfh.consolidator.Run)

	for {
		select {
		case <-ctx.Done():
			return
		case metricMaps := <-hfh.consolidatedMetrics:
			metricMap := mergeMaps(metricMaps)
			pbMetricMap := translateToProtobufV2(metricMap)
			hfh.httpClient.PostProtobuf(ctx, pbMetricMap, hfh.apiEndpoint+"/v2/raw")
		}
	}
}

func mergeMaps(maps []*gostatsd.MetricMap) *gostatsd.MetricMap {
	forwarded := false
	if len(maps) > 0 {
		forwarded = maps[0].Forwarded
	}
	mm := gostatsd.NewMetricMap(forwarded)
	for _, m := range maps {
		mm.Merge(m)
	}
	return mm
}

func translateToProtobufV2(metricMap *gostatsd.MetricMap) *pb.RawMessageV2 {
	var pbMetricMap pb.RawMessageV2

	pbMetricMap.Gauges = map[string]*pb.GaugeTagV2{}
	for metricName, m := range metricMap.Gauges {
		pbMetricMap.Gauges[metricName] = &pb.GaugeTagV2{TagMap: map[string]*pb.RawGaugeV2{}}
		for tagsKey, metric := range m {
			pbMetricMap.Gauges[metricName].TagMap[tagsKey] = &pb.RawGaugeV2{
				Tags:     metric.Tags,
				Hostname: metric.Hostname,
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
				Hostname: metric.Hostname,
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
				Hostname: metric.Hostname,
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
				Hostname:    metric.Hostname,
				SampleCount: metric.SampledCount,
				Values:      metric.Values,
			}
		}
	}

	return &pbMetricMap
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
	message := &pb.EventV2{
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

	hfh.httpClient.PostProtobuf(ctx, message, hfh.apiEndpoint+"/v2/event")

	defer hfh.eventWg.Done()
}

func (hfh *HttpForwarderHandlerV2) WaitForEvents() {
	hfh.eventWg.Wait()
}
