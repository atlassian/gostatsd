package data

import v1common "go.opentelemetry.io/proto/otlp/common/v1"

type InstrumentationScope struct {
	raw *v1common.InstrumentationScope
}

func WithInstrumentationScopeAttributes(attributes Map) func(InstrumentationScope) {
	return func(is InstrumentationScope) {
		is.raw.Attributes = attributes.unWrap()
	}
}

// NewInstrumentationScope is used for denote how the data was generated,
// this will always be set to `gostatsd` as the name, and the deployed version.
// Additional attributes can be set that could be used for filtering later on.
func NewInstrumentationScope(name string, version string, opts ...func(InstrumentationScope)) InstrumentationScope {
	is := InstrumentationScope{
		raw: &v1common.InstrumentationScope{
			Name:    name,
			Version: version,
		},
	}

	for _, opt := range opts {
		opt(is)
	}

	return is
}

func (is InstrumentationScope) Attributes() Map {
	return Map{raw: &is.raw.Attributes}
}
