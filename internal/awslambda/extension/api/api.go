package api

import (
	"path"
)

type LambdaPath string

const (
	EnvLambdaAPIKey = "AWS_LAMBDA_RUNTIME_API"
	// Version defines the used OpenAPI spec being adhere to
	// by the connecting client. It is used as part of the
	// generated URI to be used to issue connections.
	Version = "2020-01-01"

	RegisterEndpoint  LambdaPath = "/" + Version + "/extension/register"
	EventEndpoint     LambdaPath = "/" + Version + "/extension/event/next"
	InitErrorEndpoint LambdaPath = "/" + Version + "/extension/init/error"
	ExitErrorEndpoint LambdaPath = "/" + Version + "/extension/exit/error"

	Invoke   Event = "INVOKE"
	Shutdown Event = "SHUTDOWN"

	LambdaExtensionNameHeaderKey       = "Lambda-Extension-Name"
	LambdaExtensionIdentifierHeaderKey = "Lambda-Extension-Identifier"
	LambdaErrorHeaderKey               = "Lambda-Extension-Function-Error-Type"
	LambdaExtensionEventIdentifer      = "Lambda-Extension-Event-Identifier"
)

func (lp LambdaPath) GetUrl(domain string) string {
	return "http://" + path.Join(domain, string(lp))
}

func (lp LambdaPath) String() string { return string(lp) }

// Defining types used within the initialise phase of the extension
type Event string

type RegisterRequestPayload struct {
	Events []Event `json:"events"`
}

type RegisterResponsePayload struct {
	FunctionName    string `json:"functionName"`
	FunctionVersion string `json:"functionVersion"`
	Handler         string `json:"handler"`
}

// Defining types used within the next phase of the extention
type EventNextPayload struct {
	EventType          Event             `json:"eventType"`
	Deadline           int64             `json:"deadlineMs"`
	RequestID          string            `json:"requestId"`
	InvokedFunctionARN string            `json:"invokedFunctionArn"`
	Tracing            map[string]string `json:"tracing"`
	ShutdownReason     string            `json:"shutdownReason"`
}

// Defining the types used when processing errors
type ErrorRequest struct {
	Message    string   `json:"errorMessage"`
	Type       string   `json:"errorType"`
	StackTrace []string `json:"stackTrace"`
}
type ErrorResponse struct {
	Message string `json:"errorMessage"`
	Type    string `json:"errorType"`
}

type StatusResponse struct {
	Status string `json:"status"`
}
