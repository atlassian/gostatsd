package data

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	v1export "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"
)

func ProcessMetricResponse(resp *http.Response) (dropped int64, errs error) {
	if resp == nil {
		return 0, errors.New("empty response")
	}

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var response v1export.ExportMetricsServiceResponse
	if err := proto.Unmarshal(buf, &response); err != nil {
		return 0, err
	}

	if resp.StatusCode/100 != 2 {
		errs = multierr.Append(errs, fmt.Errorf("returned a non 2XX status code of %d", resp.StatusCode))
	}

	if ps := response.PartialSuccess; ps != nil && ps.ErrorMessage != "" {
		if ps.RejectedDataPoints > 0 {
			dropped = ps.RejectedDataPoints
			errs = multierr.Append(errs, fmt.Errorf("dataloss: dropped %d metrics", ps.RejectedDataPoints))
		}
		errs = multierr.Append(errs, fmt.Errorf("failed to send metrics: %s", ps.ErrorMessage))
	}

	return dropped, errs
}
