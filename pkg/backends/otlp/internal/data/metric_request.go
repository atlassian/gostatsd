package data

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"

	v1export "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

type metricsRequest struct {
	raw *v1export.ExportMetricsServiceRequest
}

func NewMetricsRequest(ctx context.Context, endpoint string, metrics []ResourceMetrics, compressPayload bool) (*http.Request, error) {
	mr := metricsRequest{
		raw: &v1export.ExportMetricsServiceRequest{
			ResourceMetrics: make([]*v1metrics.ResourceMetrics, 0, len(metrics)),
		},
	}

	for i := 0; i < len(metrics); i++ {
		mr.raw.ResourceMetrics = append(mr.raw.ResourceMetrics, metrics[i].raw)
	}

	buf, err := proto.Marshal(mr.raw)
	if err != nil {
		return nil, err
	}

	if compressPayload {
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		if _, err = w.Write(buf); err != nil {
			return nil, err
		}

		if err = w.Close(); err != nil {
			return nil, err
		}

		return createProtobufRequest(ctx, endpoint, b.Bytes(), withHeader("Content-Encoding", "gzip"))
	}

	return createProtobufRequest(ctx, endpoint, buf)
}
