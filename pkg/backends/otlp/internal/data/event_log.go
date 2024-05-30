package data

type AttributeValueType int

const (
	ValueTypeString AttributeValueType = iota
	ValueTypeInt64
	ValueTypeMap
)

type eventLogAttribute struct {
	Key       string
	Value     any
	ValueType AttributeValueType
}

type eventLogAttributes []*eventLogAttribute

func (a *eventLogAttributes) PutStr(key string, value string) {
	*a = append(*a, &eventLogAttribute{Key: key, Value: value, ValueType: ValueTypeString})
}

func (a *eventLogAttributes) PutInt(key string, value int64) {
	*a = append(*a, &eventLogAttribute{Key: key, Value: value, ValueType: ValueTypeInt64})
}

func (a *eventLogAttributes) PutMap(key string, value Map) {
	*a = append(*a, &eventLogAttribute{Key: key, Value: value, ValueType: ValueTypeMap})
}
