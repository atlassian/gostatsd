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
		name             string
		gostatsdEvent    *gostatsd.Event
		resourceKeys     []string
		wantLogRecord    *v1log.LogRecord
		wantResourceTags Map
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
			resourceKeys: []string{},
			wantLogRecord: &v1log.LogRecord{
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
						Key: "tags",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_KvlistValue{
							KvlistValue: &v1common.KeyValueList{
								Values: []*v1common.KeyValue{
									{
										Key:   "tag1",
										Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "1"}},
									},
									{
										Key:   "tag2",
										Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "2"}},
									},
								},
							},
						}},
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
					{
						Key:   "aggregation_key",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: ""}},
					},
					{
						Key:   "source_type_name",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: ""}},
					},
				},
			},
			wantResourceTags: NewMap(),
		},
		{
			name: "should allow collision of tag names with event attributes fields",
			gostatsdEvent: &gostatsd.Event{
				Title:        "title",
				Text:         "text",
				DateHappened: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
				Tags:         gostatsd.Tags{"title:1", "text:1"},
				Source:       "127.0.0.1",
				Priority:     gostatsd.PriNormal,
				AlertType:    gostatsd.AlertError,
			},
			resourceKeys: []string{},
			wantLogRecord: &v1log.LogRecord{
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
						Key: "tags",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_KvlistValue{
							KvlistValue: &v1common.KeyValueList{
								Values: []*v1common.KeyValue{
									{
										Key:   "text",
										Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "1"}},
									},
									{
										Key:   "title",
										Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "1"}},
									},
								},
							},
						}},
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
					{
						Key:   "aggregation_key",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: ""}},
					},
					{
						Key:   "source_type_name",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: ""}},
					},
				},
			},
			wantResourceTags: NewMap(),
		},
		{
			name: "should convert event log record with resource tags",
			gostatsdEvent: &gostatsd.Event{
				Title:        "title",
				Text:         "text",
				DateHappened: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
				Tags:         gostatsd.Tags{"perimeter:fedramp-moderate", "environment:prod", "title:1", "text:1"},
				Source:       "127.0.0.1",
				Priority:     gostatsd.PriNormal,
				AlertType:    gostatsd.AlertError,
			},
			resourceKeys: []string{"perimeter", "environment"},
			wantLogRecord: &v1log.LogRecord{
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
						Key: "tags",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_KvlistValue{
							KvlistValue: &v1common.KeyValueList{
								Values: []*v1common.KeyValue{
									{
										Key:   "text",
										Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "1"}},
									},
									{
										Key:   "title",
										Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "1"}},
									},
								},
							},
						}},
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
					{
						Key:   "aggregation_key",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: ""}},
					},
					{
						Key:   "source_type_name",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: ""}},
					},
				},
			},
			wantResourceTags: Map{
				raw: &[]*v1common.KeyValue{
					{
						Key:   "perimeter",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "fedramp-moderate"}},
					},
					{
						Key:   "environment",
						Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "prod"}},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewOtlpEvent(tt.gostatsdEvent)
			assert.NoError(t, err)

			record, resourceTags := s.TransformToLog(tt.resourceKeys)
			assert.Equal(t, tt.wantLogRecord.TimeUnixNano, record.TimeUnixNano)
			assert.Equal(t, len(tt.wantLogRecord.Attributes), len(record.Attributes))
			for _, kv := range tt.wantLogRecord.Attributes {
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
			for _, kv := range tt.wantResourceTags.unWrap() {
				found := false
				for _, attr := range resourceTags.unWrap() {
					if kv.Key == attr.Key {
						assert.Equal(t, kv, attr)
						found = true
						break
					}
				}
				if !found {
					t.Errorf("resource tag %s not found", kv.Key)
				}
			}
		})
	}
}
