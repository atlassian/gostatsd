package data

import (
	"time"

	v1common "go.opentelemetry.io/proto/otlp/common/v1"
	v1log "go.opentelemetry.io/proto/otlp/logs/v1"

	"github.com/atlassian/gostatsd"
)

const (
	SFxEventCategoryKey   = "com.splunk.signalfx.event_category"
	SFxEventPropertiesKey = "com.splunk.signalfx.event_properties"
	SFxEventType          = "com.splunk.signalfx.event_type"
)

// Category define how to display the Category.  Category enumerations need to be in sync with sfxmodel
type Category int32

const (
	// USERDEFINED - Created by user via UI or API, e.g. a deployment event
	USERDEFINED Category = 1000000

	// ALERT - Output by anomaly detectors
	ALERT Category = 100000
)

type AttributeValueType int

const (
	ValueTypeString AttributeValueType = iota
	ValueTypeInt64
	ValueTypeMap
)

type eventAttribute struct {
	Key       string
	Value     any
	ValueType AttributeValueType
}

type eventAttributes []*eventAttribute

func (a *eventAttributes) PutStr(key string, value string) {
	*a = append(*a, &eventAttribute{Key: key, Value: value, ValueType: ValueTypeString})
}

func (a *eventAttributes) PutInt(key string, value int64) {
	*a = append(*a, &eventAttribute{Key: key, Value: value, ValueType: ValueTypeInt64})
}

func (a *eventAttributes) PutMap(key string, value Map) {
	*a = append(*a, &eventAttribute{Key: key, Value: value, ValueType: ValueTypeMap})
}

func TransformEventToLog(e *gostatsd.Event) *v1log.LogRecord {
	attrs := eventAttributes(make([]*eventAttribute, 0))

	dimensions := e.CreateTagsMap()
	for key, value := range dimensions {
		attrs.PutStr(key, value)
	}

	attrs.PutStr(SFxEventType, e.Title)
	attrs.PutInt(SFxEventCategoryKey, int64(USERDEFINED))

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
	attrs.PutMap(SFxEventPropertiesKey, properties)

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
