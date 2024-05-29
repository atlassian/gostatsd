package data

import (
	"context"
	"net/http"

	v1export "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	v1logs "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

type eventRequest struct {
	raw *v1export.ExportLogsServiceRequest
}

func NewEventsRequest(ctx context.Context, endpoint string, events ...Events) (*http.Request, error) {
	r := eventRequest{
		raw: &v1export.ExportLogsServiceRequest{
			ResourceLogs: make([]*v1logs.ResourceLogs, 0, len(events)),
		},
	}

	for i := 0; i < len(events); i++ {
		r.raw.ResourceLogs = append(r.raw.ResourceLogs, events[i].raw)
	}

	buf, err := proto.Marshal(r.raw)
	if err != nil {
		return nil, err
	}

	return createProtobufRequest(ctx, endpoint, buf)
}
