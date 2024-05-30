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

func (s *OtlpEvent) TransformToLog() *v1log.LogRecord {
	attrs := make(map[string]string)
	e := s.raw

	dimensions := e.Tags.ToMap()
	for key, value := range dimensions {
		attrs[key] = value
	}

	attrs["title"] = e.Title
	attrs["text"] = e.Text
	attrs["source"] = string(e.Source)
	attrs["priority"] = e.Priority.String()
	attrs["alert_type"] = e.AlertType.String()

	logAttrs := make([]*v1common.KeyValue, 0)
	for k, v := range attrs {
		logAttrs = append(logAttrs, &v1common.KeyValue{
			Key: k,
			Value: &v1common.AnyValue{
				Value: &v1common.AnyValue_StringValue{
					StringValue: v,
				},
			},
		})
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

	return lr
}
