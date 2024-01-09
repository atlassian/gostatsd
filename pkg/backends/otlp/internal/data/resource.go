package data

import (
	v1common "go.opentelemetry.io/proto/otlp/common/v1"
	v1resource "go.opentelemetry.io/proto/otlp/resource/v1"
)

type Resource struct {
	embed[*v1resource.Resource]
}

func NewResource(opts ...func(Resource)) Resource {
	r := Resource{
		embed: newEmbed[*v1resource.Resource](
			func(e embed[*v1resource.Resource]) {
				e.t.Attributes = []*v1common.KeyValue{}
			},
		),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func WithResourceAttributes(opts ...func(Map)) func(Resource) {
	return func(r Resource) {
		r.Attributes().Merge(NewMap(opts...))
	}
}

func (r Resource) Attributes() Map {
	return Map{raw: &(*r.embed.t).Attributes}
}
