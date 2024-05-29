package data

import (
	"fmt"
	"io"
	"net/http"

	v1export "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"
)

func ProcessEventsResponse(resp *http.Response) (dropped int64, errs error) {
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var response v1export.ExportLogsServiceResponse
	if err := proto.Unmarshal(buf, &response); err != nil {
		return 0, err
	}

	if resp.StatusCode/100 != 2 {
		errs = multierr.Append(errs, fmt.Errorf("returned a non 2XX status code of %d", resp.StatusCode))
	}

	if ps := response.PartialSuccess; ps != nil && ps.ErrorMessage != "" {
		if ps.RejectedLogRecords > 0 {
			dropped = ps.RejectedLogRecords
			errs = multierr.Append(errs, fmt.Errorf("dataloss: dropped %d events", ps.RejectedLogRecords))
		}
		errs = multierr.Append(errs, fmt.Errorf("failed to send events: %s", ps.ErrorMessage))
	}

	return dropped, errs
}
