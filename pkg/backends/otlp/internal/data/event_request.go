package data

import (
	"context"
	"net/http"

	v1export "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	v1log "go.opentelemetry.io/proto/otlp/logs/v1"
	v1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

func NewEventsRequest(ctx context.Context, endpoint string, record *v1log.LogRecord, resourceTags Map) (*http.Request, error) {
	resourceLogs := make([]*v1log.ResourceLogs, 0, 1)
	rl := &v1log.ResourceLogs{
		Resource: &v1.Resource{
			Attributes:             *resourceTags.raw,
			DroppedAttributesCount: 0,
		},
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
