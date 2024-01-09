package data

import (
	"slices"
	"strings"

	"github.com/atlassian/gostatsd"
	v1common "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
)

type Map struct {
	raw *[]*v1common.KeyValue
}

func NewMap(opts ...func(Map)) Map {
	m := Map{
		raw: new([]*v1common.KeyValue),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// WithDelimtedStrings splits each value based on the delim passed,
// if there is no delim present, then the entire string is added to the key
// and uses an empty string as the value
func WithDelimitedStrings(delim string, values ...string) func(m Map) {
	return func(m Map) {
		for _, kv := range values {
			idx := strings.Index(kv, delim)
			switch idx {
			case -1:
				// No delimiter found
				m.Insert(kv, "")
			default:
				m.Insert(kv[:idx], kv[idx+1:])
			}
		}
	}
}

func WithStatsdDelimitedTags(tags gostatsd.Tags) func(Map) {
	return WithDelimitedStrings(":", tags...)
}

func (m Map) Equal(mm Map) bool {
	return proto.Equal(
		&v1common.KeyValueList{Values: *m.raw},
		&v1common.KeyValueList{Values: *mm.raw},
	)
}

func (m Map) Insert(key string, value string) {
	index := slices.IndexFunc(*m.raw, func(kv *v1common.KeyValue) bool {
		return kv.Key == key
	})

	val := &v1common.AnyValue{
		Value: &v1common.AnyValue_StringValue{
			StringValue: value,
		},
	}

	switch index {
	case -1:
		// The value doesn't exist within the map, appending to end
		*m.raw = append(*m.raw, &v1common.KeyValue{
			Key:   key,
			Value: val,
		})
	default:
		v, ok := (*m.raw)[index].Value.Value.(*v1common.AnyValue_ArrayValue)
		if !ok {
			// Convert a simple key value into an array of values
			v = &v1common.AnyValue_ArrayValue{
				ArrayValue: &v1common.ArrayValue{
					Values: []*v1common.AnyValue{
						{Value: v},
					},
				},
			}
		}
		v.ArrayValue.Values = append(v.ArrayValue.Values, val)
	}
}

func (m Map) Merge(mm Map) {
	for _, kv := range *mm.raw {
		switch v := kv.Value.Value.(type) {
		case *v1common.AnyValue_StringValue:
			m.Insert(kv.Key, v.StringValue)

		case *v1common.AnyValue_ArrayValue:
			for _, vv := range v.ArrayValue.Values {
				m.Insert(kv.Key, vv.GetStringValue())
			}
		}
	}
}

func (m Map) unwrap() []*v1common.KeyValue {
	return *m.raw
}
