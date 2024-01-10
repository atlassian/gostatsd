package data

import (
	"bytes"
	"context"
	"net/http"

	v1export "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	v1metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

const (
	metricRequestContentType = "application/x-protobuf"
)

type metricsRequest struct {
	raw *v1export.ExportMetricsServiceRequest
}

func NewMetricsRequest(ctx context.Context, endpoint string, metrics ...ResourceMetrics) (*http.Request, error) {
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

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint,
		bytes.NewBuffer(buf),
	)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", metricRequestContentType)

	return req, nil
}
