package lambda

import (
	"os"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"

	"github.com/atlassian/gostatsd/internal/awslambda/extension/api"
)

func TestDefaultOptions(t *testing.T) {
	t.Setenv(api.EnvLambdaAPIHostname, "example")

	expected := Options{
		RuntimeAPI:        "example",
		ExecutableName:    os.Args[0],
		EnableManualFlush: false,
		TelemetryAddr:     "http://sandbox:8083",
	}

	assert.Equal(t, expected, NewOptionsFromEnvironment(), "Must match the expected value")
}

func TestOptionsAddFlag(t *testing.T) {
	t.Parallel()

	o := &Options{
		RuntimeAPI:        "my-awesome-api",
		ExecutableName:    "bin",
		EnableManualFlush: true,
		TelemetryAddr:     ":8089",
	}

	fs := pflag.NewFlagSet(t.Name(), pflag.ContinueOnError)
	o.AddFlags(fs)

	for _, flag := range []string{
		ParamRuntimeAPI,
		ParamEntryPointName,
		ParamEnableManualFlush,
		ParamTelemetryAddr,
	} {
		assert.NotNil(
			t,
			fs.Lookup(flag),
			"Must have a valid entry for expected flag name %q",
			flag,
		)
	}

}

func TestOptionsValidate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		options Options
		errVal  string
	}{
		{
			name:    "empty options",
			options: Options{},
			errVal:  "missing `RuntimeAPI` value; missing `ExecutableName` value",
		},
		{
			name: "enabled manual flushes",
			options: Options{
				EnableManualFlush: true,
			},
			errVal: "missing `RuntimeAPI` value; missing `ExecutableName` value; missing `TelemetryAddr` when `EnableManualFlush` is enabled",
		},
		{
			name: "(simulated) Default options",
			options: Options{
				RuntimeAPI:     "runtime-api",
				ExecutableName: "bin",
			},
			errVal: "",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.options.Validate()
			if tc.errVal != "" {
				assert.EqualError(t, err, tc.errVal, "Must match the expected error message")
			} else {
				assert.NoError(t, err, "Must not error")
			}
		})
	}
}
