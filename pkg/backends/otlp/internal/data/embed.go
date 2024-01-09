package data

import "google.golang.org/protobuf/proto"

type embed[T proto.Message] struct {
	t T
}

func newEmbed[T proto.Message](opts ...func(embed[T])) embed[T] {
	var t T
	e := embed[T]{t: t}
	for i := 0; i < len(opts); i++ {
		opts[i](e)
	}
	return e
}

func (rm embed[T]) Equal(raw embed[T]) bool {
	return proto.Equal(rm.t, raw.t)
}

func (rm embed[T]) AsRaw() T {
	return rm.t
}
