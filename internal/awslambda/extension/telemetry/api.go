package telemetry

import "github.com/atlassian/gostatsd/internal/awslambda/extension/api"

const (
	ApiVersion                       = "2022-07-01"
	SubscribeEndpoint api.LambdaPath = "/" + ApiVersion + "/telemetry"
	RuntimeDoneMsg    string         = "platform.runtimeDone"

	PlatformSubscriptionType string = "platform"

	MinBufferingTimeoutMs int = 25
)

type Event struct {
	// type is the only field we deserialise as it is used for flushing metrics
	Type string `json:"type"`
}

type SubscriptionRequest struct {
	SchemaVersion string                       `json:"schemaVersion"`
	Types         []string                     `json:"types"`
	Buffering     *SubscriptionBufferingConfig `json:"buffering,omitempty"`
	Destination   *SubscriptionDestination     `json:"destination,omitempty"`
}

type SubscriptionDestination struct {
	Protocol string `json:"protocol,omitempty"`
	URI      string `json:"URI"`
}

type SubscriptionBufferingConfig struct {
	MaxItems  int         `json:"maxItems,omitempty"`
	MaxBytes  interface{} `json:"maxBytes,omitempty"`
	TimeoutMs int         `json:"timeoutMs,omitempty"`
}
