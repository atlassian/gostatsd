package data

import (
	"fmt"
	"io"
	"net/http"

	v1export "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"
)

func ProcessEventsResponse(resp *http.Response) error {
	buf, errs := io.ReadAll(resp.Body)
	if errs != nil {
		return errs
	}

	if resp.StatusCode/100 != 2 {
		errs = multierr.Append(errs, fmt.Errorf("returned a non 2XX status code of %d", resp.StatusCode))
	}

	var response v1export.ExportLogsServiceResponse
	if err := proto.Unmarshal(buf, &response); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to unmarshal response: %w", err))
		return errs
	}

	if ps := response.PartialSuccess; ps != nil && ps.ErrorMessage != "" {
		errs = multierr.Append(errs, fmt.Errorf("failed to send events: %s", ps.ErrorMessage))
	}

	return errs
}
