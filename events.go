package gostatsd

import (
	"sort"
	"strings"
	"time"

	v1common "go.opentelemetry.io/proto/otlp/common/v1"
	v1log "go.opentelemetry.io/proto/otlp/logs/v1"
)

// Priority of an event.
type Priority byte

const (
	// PriNormal is normal priority.
	PriNormal Priority = iota // Must be zero to work as default
	// PriLow is low priority.
	PriLow
)

func (p Priority) String() string {
	switch p {
	case PriLow:
		return "low"
	default:
		return "normal"
	}
}

// StringWithEmptyDefault returns empty string for default priority.
func (p Priority) StringWithEmptyDefault() string {
	switch p {
	case PriLow:
		return "low"
	default:
		return ""
	}
}

// AlertType is the type of alert.
type AlertType byte

const (
	// AlertInfo is alert level "info".
	AlertInfo AlertType = iota // Must be zero to work as default
	// AlertWarning is alert level "warning".
	AlertWarning
	// AlertError is alert level "error".
	AlertError
	// AlertSuccess is alert level "success".
	AlertSuccess
)

func (a AlertType) String() string {
	switch a {
	case AlertWarning:
		return "warning"
	case AlertError:
		return "error"
	case AlertSuccess:
		return "success"
	default:
		return "info"
	}
}

// StringWithEmptyDefault returns empty string for default alert type.
func (a AlertType) StringWithEmptyDefault() string {
	switch a {
	case AlertWarning:
		return "warning"
	case AlertError:
		return "error"
	case AlertSuccess:
		return "success"
	default:
		return ""
	}
}

// Event represents an event, described at http://docs.datadoghq.com/guides/dogstatsd/
type Event struct {
	// Title of the event.
	Title string
	// Text of the event. Supports line breaks.
	Text string
	// DateHappened of the event. Unix epoch timestamp. Default is now when not specified in incoming metric.
	DateHappened int64
	// AggregationKey of the event, to group it with some other events.
	AggregationKey string
	// SourceTypeName of the event.
	SourceTypeName string
	// Tags of the event.
	Tags Tags
	// Source of the metric
	Source Source
	// Priority of the event.
	Priority Priority
	// AlertType of the event.
	AlertType AlertType
}

func (e *Event) AddTagsSetSource(additionalTags Tags, newSource Source) {
	e.Tags = e.Tags.Concat(additionalTags)
	e.Source = newSource
}

const (
	SFxAccessTokenHeader       = "X-Sf-Token"                       // #nosec
	SFxAccessTokenLabel        = "com.splunk.signalfx.access_token" // #nosec
	SFxEventCategoryKey        = "com.splunk.signalfx.event_category"
	SFxEventPropertiesKey      = "com.splunk.signalfx.event_properties"
	SFxEventType               = "com.splunk.signalfx.event_type"
	DefaultSourceTypeLabel     = "com.splunk.sourcetype"
	DefaultSourceLabel         = "com.splunk.source"
	DefaultIndexLabel          = "com.splunk.index"
	DefaultNameLabel           = "otel.log.name"
	DefaultSeverityTextLabel   = "otel.log.severity.text"
	DefaultSeverityNumberLabel = "otel.log.severity.number"
	HECTokenHeader             = "Splunk"
	HTTPSplunkChannelHeader    = "X-Splunk-Request-Channel"
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

type EventAttribute struct {
	Key       string
	Value     any
	ValueType AttributeValueType
}

type EventAttributes []*EventAttribute

func (a *EventAttributes) PutStr(key string, value string) {
	*a = append(*a, &EventAttribute{Key: key, Value: value, ValueType: ValueTypeString})
}

func (a *EventAttributes) PutInt(key string, value int64) {
	*a = append(*a, &EventAttribute{Key: key, Value: value, ValueType: ValueTypeInt64})
}

func (a *EventAttributes) PutMap(key string, value map[string]string) {
	*a = append(*a, &EventAttribute{Key: key, Value: value, ValueType: ValueTypeMap})
}

func (e *Event) TransformToLog() *v1log.LogRecord {
	attrs := EventAttributes(make([]*EventAttribute, 0))
	var ts time.Time
	if e.DateHappened != 0 {
		ts = time.Unix(e.DateHappened, 0)
	} else {
		ts = time.Now()
	}

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

	attrs.PutMap(SFxEventPropertiesKey, map[string]string{"text": e.Text})

	ats := make([]*v1common.KeyValue, 0)
	for _, attr := range attrs {
		switch attr.ValueType {
		case ValueTypeString:
			ats = append(ats, &v1common.KeyValue{
				Key:   attr.Key,
				Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: attr.Value.(string)}},
			})
		case ValueTypeInt64:
			ats = append(ats, &v1common.KeyValue{
				Key:   attr.Key,
				Value: &v1common.AnyValue{Value: &v1common.AnyValue_IntValue{IntValue: attr.Value.(int64)}},
			})
		case ValueTypeMap:
			for k, v := range attr.Value.(map[string]string) {
				ats = append(ats, &v1common.KeyValue{
					Key:   k,
					Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: v}},
				})
			}
		}
	}

	lr := &v1log.LogRecord{
		TimeUnixNano: uint64(ts.UnixNano()),
		Attributes:   make([]*v1common.KeyValue, 0),
	}

	return lr
}

// CreateTagsMap converts all the tags into a format that can be translated
// to send to a different vendor if required.
// - If the tag exists without a value it is converted to: "unknown:<tag>"
// - If the tag has several values it is converted to: "tag:Join(Sort(values), "__")"
//   - []{ "tag:pineapple","tag:pear" } ==> "tag:pear__pineapple"
//   - []{ "tag:newt" } ==> "tag:newt"
//   - []{ "tag:newt", "tag:newt" } ==> "tag:newt__newt"
//
// - If the tag key contains a . it is re-mapped to _
func (e *Event) CreateTagsMap() map[string]string {
	flatpack := make(map[string][]string, len(e.Tags))
	for i := 0; i < len(e.Tags); i++ {
		key, value := parseTag(e.Tags[i])
		key = strings.ReplaceAll(key, `.`, `_`)
		flatpack[key] = append(flatpack[key], value)
	}
	tags := make(map[string]string, len(flatpack))
	for key, values := range flatpack {
		// Cheap operation, due to the fact that no sorting or additional allocation is required.
		if len(values) == 1 {
			tags[key] = values[0]
			continue
		}
		// Expensive operation due an values being sorted and additioanl string being created with the Join
		sort.Strings(values)
		tags[key] = strings.Join(values, `__`)
	}

	if _, exist := tags[`host`]; !exist && e.Source != UnknownSource {
		tags[`host`] = string(e.Source)
	}
	return tags
}

func parseTag(tag string) (string, string) {
	tokens := strings.SplitN(tag, ":", 2)
	if len(tokens) == 2 {
		return tokens[0], tokens[1]
	}
	return unset, tokens[0]
}

// Events represents a list of events.
type Events []*Event
