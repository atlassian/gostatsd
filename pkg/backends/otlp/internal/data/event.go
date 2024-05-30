package data

import (
	"time"

	v1common "go.opentelemetry.io/proto/otlp/common/v1"
	v1log "go.opentelemetry.io/proto/otlp/logs/v1"

	"github.com/atlassian/gostatsd"
)

// Category define how to display the Category.  Category enumerations need to be in sync with sfxmodel
type Category int32

const (
	// USERDEFINED - Created by user via UI or API, e.g. a deployment event
	USERDEFINED Category = 1000000

	// ALERT - Output by anomaly detectors
	ALERT Category = 100000
)

type OtlpEvent struct {
	raw               *gostatsd.Event
	titleAttrKey      string
	categoryAttrKey   string
	propertiesAttrKey string
}

func NewOtlpEvent(e *gostatsd.Event, opts ...Option) *OtlpEvent {
	oe := &OtlpEvent{raw: e}
	for _, opt := range opts {
		opt(oe)
	}
	return oe
}

type Option func(*OtlpEvent)

func WithTitleAttrKey(key string) func(*OtlpEvent) {
	return func(e *OtlpEvent) {
		e.titleAttrKey = key
	}
}

func WithCategoryAttrKey(key string) func(*OtlpEvent) {
	return func(e *OtlpEvent) {
		e.categoryAttrKey = key
	}
}

func WithPropertiesAttrKey(key string) func(*OtlpEvent) {
	return func(e *OtlpEvent) {
		e.propertiesAttrKey = key
	}
}

func (s *OtlpEvent) TransformToLog() *v1log.LogRecord {
	attrs := eventAttributes(make([]*eventAttribute, 0))
	e := s.raw
	dimensions := e.Tags.ToMap(string(e.Source))
	for key, value := range dimensions {
		attrs.PutStr(key, value)
	}

	if s.titleAttrKey != "" {
		attrs.PutStr(s.titleAttrKey, e.Title)
	} else {
		attrs.PutStr("title", e.Title)
	}

	if s.categoryAttrKey != "" {
		attrs.PutInt(s.categoryAttrKey, int64(USERDEFINED))
	} else {
		attrs.PutInt("category", int64(USERDEFINED))
	}

	priority := e.Priority.String()
	if priority != "" {
		attrs.PutStr("priority", e.Priority.String())
	}

	alertType := e.AlertType.String()
	if alertType != "" {
		attrs.PutStr("alert_type", e.AlertType.String())
	}

	properties := NewMap()
	properties.Insert("text", e.Text)

	if s.propertiesAttrKey != "" {
		attrs.PutMap(s.propertiesAttrKey, properties)
	} else {
		attrs.PutMap("properties", properties)
	}

	ats := make([]*v1common.KeyValue, 0)
	for _, attr := range attrs {
		switch attr.ValueType {
		case ValueTypeString:
			ats = append(ats, &v1common.KeyValue{
				Key: attr.Key,
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_StringValue{
						StringValue: attr.Value.(string),
					},
				},
			})
		case ValueTypeInt64:
			ats = append(ats, &v1common.KeyValue{
				Key: attr.Key,
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_IntValue{
						IntValue: attr.Value.(int64),
					},
				},
			})
		case ValueTypeMap:
			ats = append(ats, &v1common.KeyValue{
				Key: attr.Key,
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_KvlistValue{
						KvlistValue: &v1common.KeyValueList{
							Values: attr.Value.(Map).unWrap(),
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
		Attributes:   ats,
	}

	return lr
}
