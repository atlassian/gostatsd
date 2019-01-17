package web

import (
	"bytes"
	"compress/zlib"
	"context"
	"io/ioutil"
	"net/http"
	"sync/atomic"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pb"

	"github.com/atlassian/gostatsd/pkg/pool"
	"github.com/atlassian/gostatsd/pkg/stats"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
)

type rawHttpHandlerV1 struct {
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
	pool       *pool.MetricPool
}

func newRawHttpHandlerV1(logger logrus.FieldLogger, serverName string, handler gostatsd.PipelineHandler) *rawHttpHandlerV1 {
	return &rawHttpHandlerV1{
		logger:     logger,
		handler:    handler,
		serverName: serverName,
		pool:       pool.NewMetricPool(0), // tags will already be a slice, so we don't need to pre-allocate.
	}
}

func (rhh *rawHttpHandlerV1) RunMetrics(ctx context.Context) {
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

func (rhh *rawHttpHandlerV1) emitMetrics(statser stats.Statser) {
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

func (rhh *rawHttpHandlerV1) readBody(req *http.Request) ([]byte, int) {
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

func (rhh *rawHttpHandlerV1) MetricHandler(w http.ResponseWriter, req *http.Request) {
	b, errCode := rhh.readBody(req)

	if errCode != 0 {
		w.WriteHeader(errCode)
		return
	}

	var msg pb.RawMessageV1
	err := proto.Unmarshal(b, &msg)
	if err != nil {
		atomic.AddUint64(&rhh.requestFailureUnmarshal, 1)
		rhh.logger.WithError(err).Error("failed to unmarshal")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	for _, metric := range msg.RawMetrics {
		rm := rhh.pool.Get()

		rm.Name = metric.Name
		rm.Tags = metric.Tags
		rm.Hostname = metric.Hostname

		switch m := metric.M.(type) {
		case *pb.RawMetricV1_Counter:
			rm.Value = m.Counter.Value
			rm.Type = gostatsd.COUNTER
		case *pb.RawMetricV1_Gauge:
			rm.Value = m.Gauge.Value
			rm.Type = gostatsd.GAUGE
		case *pb.RawMetricV1_Set:
			rm.StringValue = m.Set.Value
			rm.Type = gostatsd.SET
		case *pb.RawMetricV1_Timer:
			rm.Value = m.Timer.Value
			rm.Rate = m.Timer.Rate
			rm.Type = gostatsd.TIMER
		default:
			continue
		}
		rhh.handler.DispatchMetric(req.Context(), rm)
	}

	atomic.AddUint64(&rhh.metricsProcessed, uint64(len(msg.RawMetrics)))
	atomic.AddUint64(&rhh.requestSuccess, 1)
	w.WriteHeader(http.StatusAccepted)
}

func (rhh *rawHttpHandlerV1) EventHandler(w http.ResponseWriter, req *http.Request) {
	b, errCode := rhh.readBody(req)

	if errCode != 0 {
		w.WriteHeader(errCode)
		return
	}

	var msg pb.EventV1
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
	case pb.EventV1_Normal:
		event.Priority = gostatsd.PriNormal
	case pb.EventV1_Low:
		event.Priority = gostatsd.PriLow
	default:
		event.Priority = gostatsd.PriNormal
	}

	switch msg.Type {
	case pb.EventV1_Info:
		event.AlertType = gostatsd.AlertInfo
	case pb.EventV1_Warning:
		event.AlertType = gostatsd.AlertWarning
	case pb.EventV1_Error:
		event.AlertType = gostatsd.AlertError
	case pb.EventV1_Success:
		event.AlertType = gostatsd.AlertSuccess
	default:
		event.AlertType = gostatsd.AlertInfo
	}

	rhh.handler.DispatchEvent(req.Context(), event)

	atomic.AddUint64(&rhh.eventsProcessed, 1)
	atomic.AddUint64(&rhh.requestSuccess, 1)
	w.WriteHeader(http.StatusAccepted)
}

func decompress(input []byte) ([]byte, error) {
	decompressor, err := zlib.NewReader(bytes.NewReader(input))
	if err != nil {
		return nil, err
	}
	defer decompressor.Close()

	var out bytes.Buffer
	if _, err = out.ReadFrom(decompressor); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
