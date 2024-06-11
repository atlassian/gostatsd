package otlp

import (
	"errors"
	"runtime"

	"github.com/spf13/viper"
	"go.uber.org/multierr"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/util"
)

const (
	ConversionAsGauge     = "AsGauge"
	ConversionAsHistogram = "AsHistogram"
)

type Config struct {
	gostatsd.TimerSubtypes `mapstructure:"disabled_timer_aggregations"`
	// Endpoint (Required) is the FQDN with path for the metrics ingestion endpoint
	Endpoint string `mapstructure:"endpoint"`
	// LogsEndpoint (Required) is the FQDN with path for the logs ingestion endpoint
	LogsEndpoint string `mapstructure:"logs_endpoint"`
	// MaxRequests (Optional, default: cpu.count * 2) is the upper limit on the number of inflight requests
	MaxRequests int `mapstructure:"max_requests"`
	// ResourceKeys (Optional) is used to extract values from provided tags
	// to apply to all values within a resource instead within each attribute.
	// Strongly encouraged to allow down stream consumers to
	// process based on values defined at the top level resource.
	ResourceKeys []string `mapstructure:"resource_keys"`
	// Conversion (Optional, Default: AsGauge) controls if timers and histograms are captured as histograms or gauges.
	// The options will enable the following:
	// - AsGauge     : This will emit timers withe their calculated suffixes
	//                  and histograms as gauges with the suffix `histogram`
	// - AsHistogram : This will emit timers as histograms with no buckets defined,
	//                  and histograms sets the bucket values.
	Conversion string `mapstructure:"conversion"`
	// Transport (Optional, Default: "default") is used to reference to configured transport
	// to be used for this backend
	Transport string `mapstructure:"transport"`
	// UserAgent (Optional, default: "gostatsd") allows you to set the
	// user agent header when making requests.
	UserAgent string `mapstructure:"user_agent"`
	// EventTitleAttributeKey (Optional, default: "title")
	// OTLP backend sends event in log format, this is the key used to store the title from the event in the log attributes
	EventTitleAttributeKey string `mapstructure:"event_title_attribute_key"`
	// EventPropertiesAttributeKey (Optional, default: "properties")
	// OTLP backend sends event in log format, this is the key used to store the properties from the event in the log attributes
	EventPropertiesAttributeKey string `mapstructure:"event_properties_attribute_key"`
}

func newDefaultConfig() *Config {
	return &Config{
		Transport:   "default",
		MaxRequests: runtime.NumCPU() * 2,
		Conversion:  ConversionAsGauge,
		UserAgent:   "gostatsd",
	}
}

func NewConfig(v *viper.Viper) (*Config, error) {
	cfg := newDefaultConfig()
	err := multierr.Combine(
		util.GetSubViper(v, BackendName).Unmarshal(cfg),
		cfg.Validate(),
	)

	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() (errs error) {
	if c.Endpoint == "" {
		errs = multierr.Append(errs, errors.New("no metrics endpoint defined"))
	}
	if c.LogsEndpoint == "" {
		errs = multierr.Append(errs, errors.New("no logs endpoint defined"))
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

	if _, ok := conversion[c.Conversion]; !ok {
		errs = multierr.Append(errs, errors.New(`conversion must be one of ["AsGauge", "AsHistogram"]`))
	}

	return errs
}
