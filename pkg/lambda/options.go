package lambda

import (
	"errors"
	"os"
	"path"

	"github.com/spf13/pflag"
	"go.uber.org/multierr"

	"github.com/atlassian/gostatsd/internal/awslambda/extension/api"
)

const (
	ParamRuntimeAPI        = `lambda-runtime-api`
	ParamEntryPointName    = `lamda-entrypoint-name`
	ParamEnableManualFlush = `enable-manual-flush`
	ParamTelemetryAddr     = `telemetry-addr`
)

// Options is used to provide overrides when calling `NewExtension`
type Options struct {
	// RuntimeAPI is AWS service that is responsible for orchestrating
	// the extension with the other Lambda's that are part of the deployment.
	// This will set to `env:AWS_LAMBDA_RUNTIME_API` by default as per the docs,
	// but can be overriden for testing purposes and validation.
	RuntimeAPI string
	// ExecutableName is the full file name of the lambda extension that is
	// used to validate the bootstrap process.
	ExecutableName string
	// EnableManualFlush allows for Lambda's
	// to control when the forwarder would send the accumlated metrics.
	EnableManualFlush bool
	// TelemetryAddr is used by the AWS runtime to publish
	// events back to service.
	TelemetryAddr string
}

func NewOptionsFromEnvironment() Options {
	return Options{
		RuntimeAPI:        os.Getenv(api.EnvLambdaAPIHostname),
		ExecutableName:    path.Base(os.Args[0]),
		EnableManualFlush: false,
		TelemetryAddr:     "http://sandbox:8083",
	}
}

// AddFlags is used to add preconfigured entries
// into an existing `FlagSet`
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(
		&o.RuntimeAPI,
		ParamRuntimeAPI,
		o.RuntimeAPI,
		"Sets the runtime api that is used to contact the lambda runtime service",
	)
	fs.StringVar(
		&o.ExecutableName,
		ParamEntryPointName,
		o.ExecutableName,
		"Sets the name of the executable name used within the bootstrap process.",
	)
	fs.BoolVar(
		&o.EnableManualFlush,
		ParamEnableManualFlush,
		o.EnableManualFlush,
		"When set, enables lambda(s) to force the forwarder to send data. Useful in low volume lambdas",
	)
	fs.StringVar(
		&o.TelemetryAddr,
		ParamTelemetryAddr,
		o.TelemetryAddr,
		"Allows for callbacks to force manual flushing of accumulated metrics",
	)
}

// Validate ensure all the values are valid,
// any values that not are reported as errors.
// All invalid values are reported together.
func (o Options) Validate() (errs error) {
	if o.RuntimeAPI == "" {
		errs = multierr.Append(errs, errors.New("missing `RuntimeAPI` value"))
	}
	if o.ExecutableName == "" {
		errs = multierr.Append(errs, errors.New("missing `ExecutableName` value"))
	}
	if o.EnableManualFlush && o.TelemetryAddr == "" {
		errs = multierr.Append(errs, errors.New("missing `TelemetryAddr` when `EnableManualFlush` is enabled"))
	}
	return errs
}
