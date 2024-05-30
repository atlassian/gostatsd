package data

type AttributeValueType int

const (
	ValueTypeString AttributeValueType = iota
	ValueTypeInt64
	ValueTypeMap
)

type eventAttribute struct {
	Key       string
	Value     any
	ValueType AttributeValueType
}

type eventAttributes []*eventAttribute

func (a *eventAttributes) PutStr(key string, value string) {
	*a = append(*a, &eventAttribute{Key: key, Value: value, ValueType: ValueTypeString})
}

func (a *eventAttributes) PutInt(key string, value int64) {
	*a = append(*a, &eventAttribute{Key: key, Value: value, ValueType: ValueTypeInt64})
}

func (a *eventAttributes) PutMap(key string, value Map) {
	*a = append(*a, &eventAttribute{Key: key, Value: value, ValueType: ValueTypeMap})
}
