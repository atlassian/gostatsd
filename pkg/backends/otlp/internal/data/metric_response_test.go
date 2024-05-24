package data

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1export "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
)

func TestProcessMetricsResponse(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		code    int
		body    io.Reader
		dropped int64
		errVal  string
	}{
		{
			name:    "Valid response",
			body:    bytes.NewBuffer(nil),
			code:    http.StatusOK,
			dropped: 0,
			errVal:  "",
		},
		{
			name: "Dropped data",
			code: http.StatusBadRequest,
			body: func() io.Reader {
				buf, err := proto.Marshal(&v1export.ExportMetricsServiceResponse{
					PartialSuccess: &v1export.ExportMetricsPartialSuccess{
						RejectedDataPoints: 12,
						ErrorMessage:       "missing service name",
					},
				})
				require.NoError(t, err, "Must not error when constructing test values")
				return bytes.NewBuffer(buf)
			}(),
			dropped: 12,
			errVal:  "returned a non 2XX status code of 400; dataloss: dropped 12 metrics; failed to send metrics: missing service name",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dropped, err := ProcessMetricResponse(
				&http.Response{
					StatusCode: tc.code,
					Body:       io.NopCloser(tc.body),
				},
			)
			assert.Equal(t, tc.dropped, dropped)
			if tc.errVal != "" {
				assert.EqualError(t, err, tc.errVal, "Must match the expected value")
			} else {
				assert.NoError(t, err, "Must not error")
			}
		})
	}
}
