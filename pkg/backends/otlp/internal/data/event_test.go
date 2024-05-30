package data

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1common "go.opentelemetry.io/proto/otlp/common/v1"
	v1log "go.opentelemetry.io/proto/otlp/logs/v1"

	"github.com/atlassian/gostatsd"
)

func TestTransformToLog(t *testing.T) {
	tests := []struct {
		name          string
		gostatsdEvent *gostatsd.Event
		want          *v1log.LogRecord
	}{
		{
			name: "should convert event log record with default attributes fields",
			gostatsdEvent: &gostatsd.Event{
				Title:        "title",
				Text:         "text",
				DateHappened: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
				Tags:         gostatsd.Tags{"tag1:1", "tag2:2"},
				Source:       "127.0.0.1",
				Priority:     gostatsd.PriNormal,
				AlertType:    gostatsd.AlertError,
			},
			want: &v1log.LogRecord{
				TimeUnixNano: uint64(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()),
				Attributes: []*v1common.KeyValue{
					{
						Key:   "title",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "title"}},
					},
					{
						Key:   "text",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "text"}},
					},
					{
						Key:   "tag1",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "1"}},
					},
					{
						Key:   "tag2",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "2"}},
					},
					{
						Key:   "source",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "127.0.0.1"}},
					},
					{
						Key:   "priority",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: gostatsd.PriNormal.String()}},
					},
					{
						Key:   "alert_type",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: gostatsd.AlertError.String()}},
					},
				},
			},
		},
		{
			name: "should override tags if tag collides with event attributes fields",
			gostatsdEvent: &gostatsd.Event{
				Title:        "title",
				Text:         "text",
				DateHappened: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
				Tags:         gostatsd.Tags{"title:1", "text:1"},
				Source:       "127.0.0.1",
				Priority:     gostatsd.PriNormal,
				AlertType:    gostatsd.AlertError,
			},
			want: &v1log.LogRecord{
				TimeUnixNano: uint64(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()),
				Attributes: []*v1common.KeyValue{
					{
						Key:   "title",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "title"}},
					},
					{
						Key:   "text",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "text"}},
					},
					{
						Key:   "source",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "127.0.0.1"}},
					},
					{
						Key:   "priority",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: gostatsd.PriNormal.String()}},
					},
					{
						Key:   "alert_type",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: gostatsd.AlertError.String()}},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewOtlpEvent(tt.gostatsdEvent)
			assert.NoError(t, err)

			record := s.TransformToLog()
			assert.Equal(t, tt.want.TimeUnixNano, record.TimeUnixNano)
			assert.Equal(t, len(tt.want.Attributes), len(record.Attributes))
			for _, kv := range tt.want.Attributes {
				found := false
				for _, attr := range record.Attributes {
					if kv.Key == attr.Key {
						assert.Equal(t, kv, attr)
						found = true
						break
					}
				}
				if !found {
					t.Errorf("attribute %s not found", kv.Key)
				}
			}
		})
	}
}
