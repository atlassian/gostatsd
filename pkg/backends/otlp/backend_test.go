package otlp

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1logexport "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	v1export "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	v1common "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"

	"github.com/atlassian/gostatsd/pkg/backends/otlp/internal/data"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/fixtures"
	"github.com/atlassian/gostatsd/pkg/transport"
)

type TestingWriter struct {
	tb testing.TB
}

func (tw TestingWriter) Write(p []byte) (int, error) {
	tw.tb.Log(string(p))
	return len(p), nil
}

func TestNewBackend(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		file   string
		errVal string
	}{
		{
			name:   "Correctly Configured",
			file:   "minimal.toml",
			errVal: "",
		},
		{
			name:   "All configuration options set",
			file:   "all-options.toml",
			errVal: "",
		},
		{
			name:   "No configuration set",
			file:   "empty.toml",
			errVal: "no endpoint defined",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logger := fixtures.NewTestLogger(t)

			v := viper.New()
			v.SetConfigFile(path.Join("testdata", tc.file))
			require.NoError(t, v.ReadInConfig(), "Must not error reading config")

			bk, err := NewClientFromViper(
				v,
				logger,
				transport.NewTransportPool(
					logger,
					v,
				),
			)
			if tc.errVal != "" {
				assert.Nil(t, bk, "Must have a nil backend returned")
				assert.EqualError(t, err, tc.errVal, "Must match the expected error")
			} else {
				assert.NotNil(t, bk, "Must not return a nil backend")
				assert.NoError(t, err, "Must not return an error")
			}
		})
	}
}

func TestBackendSendAsyncMetrics(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name             string
		enableHistograms bool
		mm               *gostatsd.MetricMap
		handler          http.HandlerFunc
		validate         func(t *testing.T) func(errs []error)
	}{
		{
			name: "trivial example",
			mm:   gostatsd.NewMetricMap(false),
			handler: func(w http.ResponseWriter, r *http.Request) {
				// Do nothing
			},
			validate: func(t *testing.T) func(errs []error) {
				return func(errs []error) {
					assert.Len(t, errs, 0, "Must not returned any error")
				}
			},
		},
		{
			name: "Failed non compliant resposne",
			mm:   gostatsd.NewMetricMap(false),
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "invalid data", http.StatusBadGateway)
			},
			validate: func(t *testing.T) func(errs []error) {
				return func(errs []error) {
					if !assert.Len(t, errs, 1, "Must have one error") {
						return
					}
					assert.ErrorIs(t, errs[0], proto.Error, "Must match the expected error")
				}
			},
		},
		{
			name: "Dropped data",
			mm:   gostatsd.NewMetricMap(false),
			handler: func(w http.ResponseWriter, r *http.Request) {
				resp := v1export.ExportMetricsServiceResponse{
					PartialSuccess: &v1export.ExportMetricsPartialSuccess{
						ErrorMessage:       "Failed to find auth token",
						RejectedDataPoints: 100,
					},
				}
				buf, err := proto.Marshal(&resp)
				require.NoError(t, err, "Must not fail to marshal values")
				w.Write(buf)
				w.WriteHeader(http.StatusBadRequest)
			},
			validate: func(t *testing.T) func(errs []error) {
				return func(errs []error) {
					if !assert.Len(t, errs, 2, "Must have one error") {
						return
					}
					assert.EqualError(t, errs[0], "dataloss: dropped 100 metrics")
					assert.EqualError(t, errs[1], "failed to send metrics: Failed to find auth token")
				}
			},
		},
		{
			name: "valid metric data",
			mm: func() *gostatsd.MetricMap {
				mm := gostatsd.NewMetricMap(false)
				mm.Receive(&gostatsd.Metric{
					Name:      "my-metric",
					Value:     100.0,
					Rate:      1,
					Tags:      gostatsd.Tags{"service.name:my-awesome-service"},
					Timestamp: gostatsd.Nanotime(time.Unix(100, 0).UnixNano()),
					Type:      gostatsd.COUNTER,
				})
				mm.Receive(&gostatsd.Metric{
					Name:      "my-metric",
					Value:     100.0,
					Rate:      1,
					Tags:      gostatsd.Tags{"service.name:my-awesome-service"},
					Timestamp: gostatsd.Nanotime(time.Unix(100, 0).UnixNano()),
					Type:      gostatsd.GAUGE,
				})
				mm.Receive(&gostatsd.Metric{
					Name:      "my-metric",
					Value:     100.0,
					Rate:      1,
					Tags:      gostatsd.Tags{"service.name:my-awesome-service"},
					Timestamp: gostatsd.Nanotime(time.Unix(100, 0).UnixNano()),
					Type:      gostatsd.SET,
				})
				mm.Receive(&gostatsd.Metric{
					Name:      "my-metric",
					Value:     100.0,
					Rate:      1,
					Tags:      gostatsd.Tags{"service.name:my-awesome-service"},
					Timestamp: gostatsd.Nanotime(time.Unix(100, 0).UnixNano()),
					Type:      gostatsd.TIMER,
				})
				return mm
			}(),
			handler: func(w http.ResponseWriter, r *http.Request) {
				// Do nothing
			},
			validate: func(t *testing.T) func(errs []error) {
				return func(errs []error) {
					if !assert.Len(t, errs, 0, "Must not error") {
						return
					}
				}
			},
		},
		{
			name: "valid metric data with histogram conversion",
			mm: func() *gostatsd.MetricMap {
				mm := gostatsd.NewMetricMap(false)
				mm.Receive(&gostatsd.Metric{
					Name:      "my-metric",
					Value:     100.0,
					Rate:      1,
					Tags:      gostatsd.Tags{"service.name:my-awesome-service"},
					Timestamp: gostatsd.Nanotime(time.Unix(100, 0).UnixNano()),
					Type:      gostatsd.COUNTER,
				})
				mm.Receive(&gostatsd.Metric{
					Name:      "my-metric",
					Value:     100.0,
					Rate:      1,
					Tags:      gostatsd.Tags{"service.name:my-awesome-service"},
					Timestamp: gostatsd.Nanotime(time.Unix(100, 0).UnixNano()),
					Type:      gostatsd.GAUGE,
				})
				mm.Receive(&gostatsd.Metric{
					Name:      "my-metric",
					Value:     100.0,
					Rate:      1,
					Tags:      gostatsd.Tags{"service.name:my-awesome-service"},
					Timestamp: gostatsd.Nanotime(time.Unix(100, 0).UnixNano()),
					Type:      gostatsd.SET,
				})
				mm.Receive(&gostatsd.Metric{
					Name:      "my-metric",
					Value:     100.0,
					Rate:      1,
					Tags:      gostatsd.Tags{"service.name:my-awesome-service"},
					Timestamp: gostatsd.Nanotime(time.Unix(100, 0).UnixNano()),
					Type:      gostatsd.TIMER,
				})
				return mm
			}(),
			handler: func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				assert.NoError(t, err, "Must not error reading body")
				assert.NotEmpty(t, body, "Must not have an empty body")
			},
			enableHistograms: true,
			validate: func(t *testing.T) func(errs []error) {
				return func(errs []error) {
					if !assert.Len(t, errs, 0, "Must not error") {
						return
					}
				}
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := httptest.NewServer(tc.handler)
			t.Cleanup(s.Close)

			v := viper.New()
			v.Set("otlp.endpoint", s.URL)
			if tc.enableHistograms {
				v.Set("otlp.conversion", ConversionAsHistogram)
			}

			logger := fixtures.NewTestLogger(t)

			b, err := NewClientFromViper(
				v,
				logger,
				transport.NewTransportPool(logger, v),
			)
			require.NoError(t, err, "Must not error creating backend")

			b.SendMetricsAsync(context.Background(), tc.mm, tc.validate(t))
		})
	}
}

func TestSendEvent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		event   *gostatsd.Event
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "should send event as log with correct attributes",
			handler: func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				assert.NoError(t, err, "Must not error reading body")
				assert.NotEmpty(t, body, "Must not have an empty body")

				req := &v1logexport.ExportLogsServiceRequest{}
				err = proto.Unmarshal(body, req)
				assert.NoError(t, err, "Must not error unmarshalling body")

				record := req.ResourceLogs[0].ScopeLogs[0].LogRecords[0]

				assert.Equal(t, uint64(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()), record.TimeUnixNano)

				// event title is stored in SFxEventType ("com.splunk.signalfx.event_type")
				assert.Equal(t, &v1common.AnyValue_StringValue{StringValue: "test title"}, findAttrByKey(record.Attributes, data.SFxEventType))

				// text is stored in SFxEventProperties ("com.splunk.signalfx.event_properties")
				assert.Equal(t, &v1common.AnyValue_KvlistValue{
					KvlistValue: &v1common.KeyValueList{
						Values: []*v1common.KeyValue{
							{
								Key:   "text",
								Value: &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: "test text"}},
							},
						},
					},
				}, findAttrByKey(record.Attributes, data.SFxEventPropertiesKey))

				assert.Equal(t, &v1common.AnyValue_StringValue{StringValue: "my-tag"}, findAttrByKey(record.Attributes, "tag"))
				assert.Equal(t, &v1common.AnyValue_StringValue{StringValue: "127.0.0.1"}, findAttrByKey(record.Attributes, "host"))
				assert.Equal(t, &v1common.AnyValue_StringValue{StringValue: gostatsd.PriNormal.String()}, findAttrByKey(record.Attributes, "priority"))
				assert.Equal(t, &v1common.AnyValue_StringValue{StringValue: gostatsd.AlertError.String()}, findAttrByKey(record.Attributes, "alert_type"))
			},
			event: &gostatsd.Event{
				Title:        "test title",
				Text:         "test text",
				DateHappened: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
				Tags:         gostatsd.Tags{"tag:my-tag"},
				Source:       "127.0.0.1",
				Priority:     gostatsd.PriNormal,
				AlertType:    gostatsd.AlertError,
			},
			wantErr: assert.NoError,
		},
		{
			name: "should return error when server returns non 2XX status code",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "im dead", http.StatusServiceUnavailable)
			},
			event:   &gostatsd.Event{},
			wantErr: assert.Error,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := httptest.NewServer(tc.handler)
			t.Cleanup(s.Close)

			v := viper.New()
			v.Set("otlp.endpoint", s.URL)

			logger := fixtures.NewTestLogger(t)

			b, err := NewClientFromViper(
				v,
				logger,
				transport.NewTransportPool(logger, v),
			)
			require.NoError(t, err, "Must not error creating backend")

			tc.wantErr(t, b.SendEvent(context.Background(), tc.event))
		})
	}
}

func findAttrByKey(attributes []*v1common.KeyValue, s string) any {
	for _, attr := range attributes {
		if attr.Key == s {
			return attr.Value.GetValue()
		}
	}
	return nil
}
