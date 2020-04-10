package web

import (
	"context"
	"io/ioutil"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pb"

	"github.com/atlassian/gostatsd/pkg/stats"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
)

type rawHttpHandlerV2 struct {
	requestSuccess           uint64 // atomic
	requestFailureRead       uint64 // atomic
	requestFailureDecompress uint64 // atomic
	requestFailureEncoding   uint64 // atomic
	requestFailureUnmarshal  uint64 // atomic
	metricsProcessed         uint64 // atomic
	eventsProcessed          uint64 // atomic

	logger     logrus.FieldLogger
	handler    gostatsd.PipelineHandler
	serverName string
}

func newRawHttpHandlerV2(logger logrus.FieldLogger, serverName string, handler gostatsd.PipelineHandler) *rawHttpHandlerV2 {
	return &rawHttpHandlerV2{
		logger:     logger,
		handler:    handler,
		serverName: serverName,
	}
}

func (rhh *rawHttpHandlerV2) RunMetricsContext(ctx context.Context) {
	statser := stats.FromContext(ctx).WithTags([]string{"server-name:" + rhh.serverName})

	notify, cancel := statser.RegisterFlush()
	defer cancel()

	for {
		select {
		case <-notify:
			rhh.emitMetrics(statser)
		case <-ctx.Done():
			return
		}
	}
}

func (rhh *rawHttpHandlerV2) emitMetrics(statser stats.Statser) {
	requestSuccess := atomic.SwapUint64(&rhh.requestSuccess, 0)
	requestFailureRead := atomic.SwapUint64(&rhh.requestFailureRead, 0)
	requestFailureDecompress := atomic.SwapUint64(&rhh.requestFailureDecompress, 0)
	requestFailureEncoding := atomic.SwapUint64(&rhh.requestFailureEncoding, 0)
	requestFailureUnmarshal := atomic.SwapUint64(&rhh.requestFailureUnmarshal, 0)
	metricsProcessed := atomic.SwapUint64(&rhh.metricsProcessed, 0)
	eventsProcessed := atomic.SwapUint64(&rhh.eventsProcessed, 0)

	statser.Count("http.incoming", float64(requestSuccess), []string{"result:success"})
	statser.Count("http.incoming", float64(requestFailureRead), []string{"result:failure", "failure:read"})
	statser.Count("http.incoming", float64(requestFailureDecompress), []string{"result:failure", "failure:decompress"})
	statser.Count("http.incoming", float64(requestFailureEncoding), []string{"result:failure", "failure:encoding"})
	statser.Count("http.incoming", float64(requestFailureUnmarshal), []string{"result:failure", "failure:unmarshal"})
	statser.Count("http.incoming.metrics", float64(metricsProcessed), nil)
	statser.Count("http.incoming.events", float64(eventsProcessed), nil)
}

func (rhh *rawHttpHandlerV2) readBody(req *http.Request) ([]byte, int) {
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		atomic.AddUint64(&rhh.requestFailureRead, 1)
		rhh.logger.WithError(err).Info("failed reading body")
		return nil, http.StatusInternalServerError
	}
	req.Body.Close()

	encoding := req.Header.Get("Content-Encoding")
	switch encoding {
	case "deflate":
		b, err = decompress(b)
		if err != nil {
			atomic.AddUint64(&rhh.requestFailureDecompress, 1)
			rhh.logger.WithError(err).Info("failed decompressing body")
			return nil, http.StatusBadRequest
		}
	case "identity", "":
		// no action
	default:
		atomic.AddUint64(&rhh.requestFailureEncoding, 1)
		if len(encoding) > 64 {
			encoding = encoding[0:64]
		}
		rhh.logger.WithField("encoding", encoding).Info("invalid encoding")
		return nil, http.StatusBadRequest
	}

	return b, 0
}

func (rhh *rawHttpHandlerV2) MetricHandler(w http.ResponseWriter, req *http.Request) {
	b, errCode := rhh.readBody(req)

	if errCode != 0 {
		w.WriteHeader(errCode)
		return
	}

	var msg pb.RawMessageV2
	err := proto.Unmarshal(b, &msg)
	if err != nil {
		atomic.AddUint64(&rhh.requestFailureUnmarshal, 1)
		rhh.logger.WithError(err).Error("failed to unmarshal")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	mm := translateFromProtobufV2(&msg)
	rhh.handler.DispatchMetricMap(req.Context(), mm)

	atomic.AddUint64(&rhh.requestSuccess, 1)
	w.WriteHeader(http.StatusAccepted)
}

func (rhh *rawHttpHandlerV2) EventHandler(w http.ResponseWriter, req *http.Request) {
	b, errCode := rhh.readBody(req)

	if errCode != 0 {
		w.WriteHeader(errCode)
		return
	}

	var msg pb.EventV2
	err := proto.Unmarshal(b, &msg)
	if err != nil {
		atomic.AddUint64(&rhh.requestFailureUnmarshal, 1)
		rhh.logger.WithError(err).Error("failed to unmarshal")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	event := &gostatsd.Event{
		Title:          msg.Title,
		Text:           msg.Text,
		DateHappened:   msg.DateHappened,
		Hostname:       msg.Hostname,
		AggregationKey: msg.AggregationKey,
		SourceTypeName: msg.SourceTypeName,
		Tags:           msg.Tags,
		SourceIP:       gostatsd.IP(msg.SourceIP),
	}

	switch msg.Priority {
	case pb.EventV2_Normal:
		event.Priority = gostatsd.PriNormal
	case pb.EventV2_Low:
		event.Priority = gostatsd.PriLow
	default:
		event.Priority = gostatsd.PriNormal
	}

	switch msg.Type {
	case pb.EventV2_Info:
		event.AlertType = gostatsd.AlertInfo
	case pb.EventV2_Warning:
		event.AlertType = gostatsd.AlertWarning
	case pb.EventV2_Error:
		event.AlertType = gostatsd.AlertError
	case pb.EventV2_Success:
		event.AlertType = gostatsd.AlertSuccess
	default:
		event.AlertType = gostatsd.AlertInfo
	}

	rhh.handler.DispatchEvent(req.Context(), event)

	atomic.AddUint64(&rhh.eventsProcessed, 1)
	atomic.AddUint64(&rhh.requestSuccess, 1)
	w.WriteHeader(http.StatusAccepted)
}

func translateFromProtobufV2(pbMetricMap *pb.RawMessageV2) *gostatsd.MetricMap {
	now := gostatsd.Nanotime(time.Now().UnixNano())
	mm := gostatsd.NewMetricMap()

	for metricName, tagMap := range pbMetricMap.Gauges {
		mm.Gauges[metricName] = map[string]gostatsd.Gauge{}
		for tagsKey, gauge := range tagMap.TagMap {
			mm.Gauges[metricName][tagsKey] = gostatsd.Gauge{
				Value:     gauge.Value,
				Timestamp: now,
				Hostname:  gauge.Hostname,
				Tags:      gauge.Tags,
			}
		}
	}

	for metricName, tagMap := range pbMetricMap.Counters {
		mm.Counters[metricName] = map[string]gostatsd.Counter{}
		for tagsKey, counter := range tagMap.TagMap {
			mm.Counters[metricName][tagsKey] = gostatsd.Counter{
				Value:     counter.Value,
				Timestamp: now,
				Tags:      counter.Tags,
				Hostname:  counter.Hostname,
			}
		}
	}

	for metricName, tagMap := range pbMetricMap.Timers {
		mm.Timers[metricName] = map[string]gostatsd.Timer{}
		for tagsKey, timer := range tagMap.TagMap {
			mm.Timers[metricName][tagsKey] = gostatsd.Timer{
				Values:       timer.Values,
				Timestamp:    now,
				Tags:         timer.Tags,
				Hostname:     timer.Hostname,
				SampledCount: timer.SampleCount,
			}
		}
	}

	for metricName, tagMap := range pbMetricMap.Sets {
		mm.Sets[metricName] = map[string]gostatsd.Set{}
		for tagsKey, set := range tagMap.TagMap {
			mm.Sets[metricName][tagsKey] = gostatsd.Set{
				Values:    map[string]struct{}{},
				Timestamp: now,
				Tags:      set.Tags,
				Hostname:  set.Hostname,
			}
			for _, value := range set.Values {
				mm.Sets[metricName][tagsKey].Values[value] = struct{}{}
			}
		}
	}

	return mm
}
