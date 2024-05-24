package data

import (
	v1resource "go.opentelemetry.io/proto/otlp/resource/v1"
)

type Resource struct {
	raw *v1resource.Resource
}

func WithResourceAttributes(opts ...func(Map)) func(Resource) {
	return func(r Resource) {
		r.Attributes().Merge(NewMap(opts...))
	}
}

func WithResourceMap(m Map) func(Resource) {
	return func(r Resource) {
		r.Attributes().Merge(m)
	}
}

func NewResource(opts ...func(Resource)) Resource {
	r := Resource{
		raw: &v1resource.Resource{},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r Resource) Attributes() Map {
	return Map{raw: &(*r.raw).Attributes}
}
