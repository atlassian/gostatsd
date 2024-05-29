package gostatsd

import (
	"sort"
	"strings"
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
