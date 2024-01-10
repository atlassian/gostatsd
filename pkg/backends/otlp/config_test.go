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
			errVal: "no endpoint defined",
		},
		{
			name: "min configuration defined",
			v: func() *viper.Viper {
				v := viper.New()
				v.SetDefault("endpoint", "http://local")
				v.SetDefault("max_requests", 1)
				return v
			}(),
			expect: &Config{
				Endpoint:            "http://local",
				MaxRequests:         1,
				TimerConversion:     "AsGauge",
				HistogramConversion: "AsGauge",
				Transport:           "default",
				UserAgent:           "gostatsd",
			},
			errVal: "",
		},
		{
			name: "invalid options",
			v: func() *viper.Viper {
				v := viper.New()
				v.SetDefault("max_requests", 0)
				v.SetDefault("transport", "")
				v.SetDefault("timer_conversion", "values")
				v.SetDefault("histogram_conversion", "values")
				return v
			}(),
			expect: nil,
			errVal: "no endpoint defined; max request must be a positive value; no transport defined; time conversion must be one of [\"AsGauge\", \"AsHistogram\"]; histogram conversion must be one of [\"AsGauge\", \"AsHistogram\"]",
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
