package gostatsd

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

// Events represents a list of events.
type Events []*Event
