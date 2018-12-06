package stats

import (
	"context"
)

type statserKey int

const statserContextKey = statserKey(0)

var nullStatser = &NullStatser{}

// NewContext attaches a Statser to a Context
func NewContext(ctx context.Context, statser Statser) context.Context {
	return context.WithValue(ctx, statserContextKey, statser)
}

// FromContext returns a Statser from a Context.  Always succeeds, will return a NullStatser if there is no
// statser present.
func FromContext(ctx context.Context) Statser {
	if statser, ok := ctx.Value(statserContextKey).(Statser); ok {
		return statser
	}
	return nullStatser
}
