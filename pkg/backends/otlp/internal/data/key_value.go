package data

import (
	"slices"
	"strings"

	v1common "go.opentelemetry.io/proto/otlp/common/v1"
)

type KeyValue struct {
	raw *v1common.KeyValue
}

func newKeyValue(raw *v1common.KeyValue) KeyValue {
	return KeyValue{raw: raw}
}

// Compare is used to determine order of a KeyValue
// within `Map` value, the keys are compared first and
// if they match, then values are compared.
// In the event that value types do not match,
// the `ArrayValue` takes priority of the `StringValue`.
func (a KeyValue) Compare(b KeyValue) int {
	if cmp := strings.Compare(a.raw.Key, b.raw.Key); cmp != 0 {
		return cmp
	}

	aStr, aOK := a.raw.Value.Value.(*v1common.AnyValue_StringValue)
	bStr, bOK := b.raw.Value.Value.(*v1common.AnyValue_StringValue)
	switch {
	case aOK && bOK:
		return strings.Compare(aStr.StringValue, bStr.StringValue)
	case aOK && !bOK:
		return -1
	case !aOK && bOK:
		return +1
	}

	return slices.CompareFunc(
		a.raw.Value.GetArrayValue().Values,
		b.raw.Value.GetArrayValue().Values,
		func(a, b *v1common.AnyValue) int {
			return strings.Compare(a.GetStringValue(), b.GetStringValue())
		},
	)
}

// InsertValue will update the value to be an array value if
// values doesn't match the existing value, and the array
// values are ensured for their uniquiness.
func (kv KeyValue) InsertValue(value string) {
	if value == "" || kv.raw.Value.GetStringValue() == value {
		return
	}
	val := &v1common.AnyValue{
		Value: &v1common.AnyValue_StringValue{
			StringValue: value,
		},
	}
	switch v := kv.raw.Value.Value.(type) {
	case *v1common.AnyValue_StringValue:
		b := &v1common.AnyValue{
			Value: &v1common.AnyValue_StringValue{
				StringValue: v.StringValue,
			},
		}
		kv.raw.Value.Value = &v1common.AnyValue_ArrayValue{
			ArrayValue: &v1common.ArrayValue{
				Values: []*v1common.AnyValue{
					minAnyValue(b, val),
					maxAnyValue(b, val),
				},
			},
		}
	case *v1common.AnyValue_ArrayValue:
		index, found := slices.BinarySearchFunc(v.ArrayValue.Values, val, func(a, b *v1common.AnyValue) int {
			return strings.Compare(a.GetStringValue(), b.GetStringValue())
		})
		if !found {
			v.ArrayValue.Values = slices.Insert(v.ArrayValue.Values, index, val)
		}
	}
}

func minAnyValue(a, b *v1common.AnyValue) *v1common.AnyValue {
	if a.GetStringValue() < b.GetStringValue() {
		return a
	}
	return b
}

func maxAnyValue(a, b *v1common.AnyValue) *v1common.AnyValue {
	if a.GetStringValue() > b.GetStringValue() {
		return a
	}
	return b
}
