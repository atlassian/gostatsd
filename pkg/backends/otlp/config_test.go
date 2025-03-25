package otlp

import (
	"testing"
	"time"

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
				v.SetDefault("otlp.metrics_endpoint", "http://local/v1/metrics")
				v.SetDefault("otlp.logs_endpoint", "http://local/v1/logs")
				v.SetDefault("otlp.max_requests", 1)
				v.SetDefault("otlp.max_retries", 3)
				v.SetDefault("otlp.max_request_elapsed_time", "15s")
				v.SetDefault("otlp.compress_payload", true)
				v.SetDefault("otlp.metrics_per_batch", 999)
				return v
			}(),
			expect: &Config{
				MetricsEndpoint:       "http://local/v1/metrics",
				LogsEndpoint:          "http://local/v1/logs",
				MaxRequests:           1,
				MaxRetries:            3,
				MaxRequestElapsedTime: time.Second * 15,
				CompressPayload:       true,
				MetricsPerBatch:       999,
				Conversion:            "AsGauge",
				Transport:             "default",
				UserAgent:             "gostatsd",
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
