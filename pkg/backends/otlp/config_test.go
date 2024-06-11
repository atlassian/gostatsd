package otlp

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestNewConfig(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		v      *viper.Viper
		expect *Config
		errVal string
	}{
		{
			name:   "empty configuration",
			v:      viper.New(),
			expect: nil,
			errVal: "no metrics endpoint defined; no logs endpoint defined",
		},
		{
			name: "min configuration defined",
			v: func() *viper.Viper {
				v := viper.New()
				v.SetDefault("otlp.endpoint", "http://local/v1/metrics")
				v.SetDefault("otlp.logs_endpoint", "http://local/v1/logs")
				v.SetDefault("otlp.max_requests", 1)
				return v
			}(),
			expect: &Config{
				Endpoint:     "http://local/v1/metrics",
				LogsEndpoint: "http://local/v1/logs",
				MaxRequests:  1,
				Conversion:   "AsGauge",
				Transport:    "default",
				UserAgent:    "gostatsd",
			},
			errVal: "",
		},
		{
			name: "invalid options",
			v: func() *viper.Viper {
				v := viper.New()
				v.SetDefault("otlp.max_requests", 0)
				v.SetDefault("otlp.transport", "")
				v.SetDefault("otlp.conversion", "values")
				v.SetDefault("otlp.histogram_conversion", "values")
				return v
			}(),
			expect: nil,
			errVal: "no metrics endpoint defined; no logs endpoint defined; max request must be a positive value; no transport defined; conversion must be one of [\"AsGauge\", \"AsHistogram\"]",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := NewConfig(tc.v)
			assert.Equal(t, tc.expect, cfg, "Must match the expected configuration")
			if tc.errVal != "" {
				assert.EqualError(t, err, tc.errVal, "Must match the expected error")
			} else {
				assert.NoError(t, err, "Must not error")
			}
		})
	}
}
