package otlp

import (
	"errors"
	"runtime"

	"github.com/spf13/viper"
	"go.uber.org/multierr"
)

const (
	ConversionAsGauge     = "AsGauge"
	ConversionAsHistogram = "AsHistogram"
)

type Config struct {
	// Endpoint (Required) is the FQDN with path for the metrics ingestion endpoint
	Endpoint string `mapstructure:"endpoint"`
	// MaxRequests (Optional, default: cpu.count * 2) is the upper limit on the number of inflight requests
	MaxRequests int `mapstructure:"max_requests"`
	// ResourceKeys (Optional) is used to extract values from provided tags
	// to apply to all values within a resource instead within each attribute.
	// Strongly encouraged to allow down stream consumers to
	// process based on values defined at the top level resource.
	ResourceKeys []string `mapstructure:"resource_keys"`
	// TimerConversion (Optional, Default: AsGauge) determines if a timers are emitted as one of the following:
	// - AsGauges emits each calcauted value (min, max, sum, count, upper, upper_99, etc...) as a guage value
	// - AsHistograms each timer is set as an empty bucket histogram with only Min, Max, Sum, and Count set
	TimerConversion string `mapstructure:"timer_conversion"`
	// HistogramConversion (Optional, Default: AsGauge) determines how a GoStatsD histogram should be converted into OTLP.
	// The allowed values for this are:
	// - AsGauges: sends each bucket count (including +inf) as a guage value and the bucket boundry as a tag
	// - AsHistogram: sends each histogram as a single Histogram value with bucket and statistics calculated.
	HistogramConversion string `mapstructure:"histogram_conversion"`
	// Transport (Optional, Default: "default") is used to reference to configured transport
	// to be used for this backend
	Transport string `mapstructure:"transport"`
	// UserAgent (Optional, default: "gostatsd") allows you to set the
	// user agent header when making requests.
	UserAgent string `mapstructure:"user_agent"`
}

func newDefaultConfig() Config {
	return Config{
		Transport:           "default",
		MaxRequests:         runtime.NumCPU() * 2,
		TimerConversion:     "AsGauge",
		HistogramConversion: "AsGauge",
		UserAgent:           "gostatsd",
	}
}

func NewConfig(v *viper.Viper) (*Config, error) {
	cfg := newDefaultConfig()
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c Config) Validate() (errs error) {
	if c.Endpoint == "" {
		errs = multierr.Append(errs, errors.New("no endpoint defined"))
	}
	if c.MaxRequests <= 0 {
		errs = multierr.Append(errs, errors.New("max request must be a positive value"))
	}
	if c.Transport == "" {
		errs = multierr.Append(errs, errors.New("no transport defined"))
	}

	conversion := map[string]struct{}{
		ConversionAsGauge:     {},
		ConversionAsHistogram: {},
	}

	if _, ok := conversion[c.TimerConversion]; !ok {
		errs = multierr.Append(errs, errors.New(`time conversion must be one of ["AsGauge", "AsHistogram"]`))
	}

	if _, ok := conversion[c.HistogramConversion]; !ok {
		errs = multierr.Append(errs, errors.New(`histogram conversion must be one of ["AsGauge", "AsHistogram"]`))
	}

	return errs
}
