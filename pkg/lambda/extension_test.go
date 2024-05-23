package lambda

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/atlassian/gostatsd/pkg/statsd"
)

func TestNewExtension(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		options Options
		errVal  string
	}{
		{
			name:    "no options value defined",
			options: Options{},
			errVal:  "missing `RuntimeAPI` value; missing `ExecutableName` value",
		},
		{
			name: "valid options provided",
			options: Options{
				RuntimeAPI:     "runtime-api",
				ExecutableName: "bin",
			},
			errVal: "",
		},
		{
			name: "valid options provided with enable manual flush",
			options: Options{
				RuntimeAPI:        "runtime-api",
				ExecutableName:    "bin",
				EnableManualFlush: true,
				TelemetryAddr:     ":0",
			},
			errVal: "",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			actual, err := NewExtension(logrus.New(), &statsd.Server{}, tc.options)
			if tc.errVal != "" {
				assert.Nil(t, actual, "Must not be a valid extension")
				assert.EqualError(t, err, tc.errVal)
			} else {
				assert.NotNil(t, actual, "Must be a valid extension")
				assert.NoError(t, err, "Must not error")
			}
		})
	}
}
