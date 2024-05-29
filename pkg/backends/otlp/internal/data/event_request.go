package data

import (
	"context"
	"net/http"

	v1export "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	v1log "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

func NewEventsRequest(ctx context.Context, endpoint string, record *v1log.LogRecord) (*http.Request, error) {
	resourceLogs := make([]*v1log.ResourceLogs, 0, 1)
	rl := &v1log.ResourceLogs{
		Resource: nil,
		ScopeLogs: []*v1log.ScopeLogs{
			{
				LogRecords: []*v1log.LogRecord{
					record,
				},
			},
		},
	}

	resourceLogs = append(resourceLogs, rl)

	rawReq := &v1export.ExportLogsServiceRequest{
		ResourceLogs: resourceLogs,
	}

	buf, err := proto.Marshal(rawReq)
	if err != nil {
		return nil, err
	}

	return createProtobufRequest(ctx, endpoint, buf)
}
