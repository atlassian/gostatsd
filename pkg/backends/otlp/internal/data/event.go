package data

import (
	"errors"
	"time"

	v1common "go.opentelemetry.io/proto/otlp/common/v1"
	v1log "go.opentelemetry.io/proto/otlp/logs/v1"

	"github.com/atlassian/gostatsd"
)

type OtlpEvent struct {
	raw *gostatsd.Event
}

func NewOtlpEvent(e *gostatsd.Event) (*OtlpEvent, error) {
	if e == nil {
		return nil, errors.New("event not found")
	}
	return &OtlpEvent{raw: e}, nil
}

func (s *OtlpEvent) TransformToLog(resourceKeys []string) (*v1log.LogRecord, Map) {
	attrs := make(map[string]any)
	e := s.raw

	attrs["title"] = e.Title
	attrs["text"] = e.Text
	attrs["source"] = string(e.Source)
	attrs["priority"] = e.Priority.String()
	attrs["alert_type"] = e.AlertType.String()
	attrs["aggregation_key"] = e.AggregationKey
	attrs["source_type_name"] = e.SourceTypeName

	resourceTags, tags := SplitEventTagsByKeys(e.Tags, resourceKeys)
	attrs["tags"] = tags

	logAttrs := make([]*v1common.KeyValue, 0)
	for k, v := range attrs {
		switch v.(type) {
		case string:
			logAttrs = append(logAttrs, &v1common.KeyValue{
				Key: k,
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_StringValue{
						StringValue: v.(string),
					},
				},
			})
		case Map:
			logAttrs = append(logAttrs, &v1common.KeyValue{
				Key: k,
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_KvlistValue{
						KvlistValue: &v1common.KeyValueList{
							Values: tags.unWrap(),
						},
					},
				},
			})
		}
	}

	var ts time.Time
	if e.DateHappened != 0 {
		ts = time.Unix(e.DateHappened, 0)
	} else {
		ts = time.Now()
	}

	lr := &v1log.LogRecord{
		TimeUnixNano: uint64(ts.UnixNano()),
		Attributes:   logAttrs,
	}

	return lr, resourceTags
}
