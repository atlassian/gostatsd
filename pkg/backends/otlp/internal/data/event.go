package data

import (
	v1logs "go.opentelemetry.io/proto/otlp/logs/v1"
)

type Events struct {
	raw *v1logs.ResourceLogs
}

func NewEvents() Events {
	return Events{
		//raw: &v1logs.,
	}
}
