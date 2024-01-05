package data

import (
	"hash/fnv"
	"io"
	"slices"
	"strings"

	v1common "go.opentelemetry.io/proto/otlp/common/v1"
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
func WithDelimitedStrings[Tags ~[]string](delim string, tags Tags) func(m Map) {
	return func(m Map) {
		for _, kv := range tags {
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

func WithStatsdDelimitedTags[Tags ~[]string](tags Tags) func(Map) {
	return WithDelimitedStrings(":", tags)
}

func (m Map) Hash() uint64 {
	hash := fnv.New64()
	for i := 0; i < len(*m.raw); i++ {
		_, _ = io.WriteString(hash, (*m.raw)[i].Key)
		switch val := (*m.raw)[i].Value.Value.(type) {
		case *v1common.AnyValue_StringValue:
			_, _ = io.WriteString(hash, val.StringValue)
		case *v1common.AnyValue_ArrayValue:
			for _, v := range val.ArrayValue.Values {
				_, _ = io.WriteString(hash, v.GetStringValue())
			}
		}
	}
	return hash.Sum64()
}

// Insert will ensure that keys and their values are distinct.
func (m Map) Insert(key string, value string) {
	kv := &v1common.KeyValue{
		Key: key,
		Value: &v1common.AnyValue{
			Value: &v1common.AnyValue_StringValue{
				StringValue: value,
			},
		},
	}

	index, found := slices.BinarySearchFunc(*m.raw, kv, func(a, b *v1common.KeyValue) int {
		return strings.Compare(a.Key, b.Key)
	})
	if !found {
		*m.raw = slices.Insert(*m.raw, index, kv)
		return
	}
	newKeyValue((*m.raw)[index]).InsertValue(value)
}

func (m Map) Merge(mm Map) {
	// In the event that no values are set
	// for `m`, then we can copy the reference
	if len((*m.raw)) == 0 {
		*m.raw = make([]*v1common.KeyValue, len(*mm.raw))
		copy(*m.raw, *mm.raw)
		return
	}
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

func (m Map) unWrap() []*v1common.KeyValue {
	return *m.raw
}
