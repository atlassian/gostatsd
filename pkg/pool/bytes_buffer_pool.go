package pool

import (
	"bytes"
	"sync"
)

// BytesBuffer is a strongly typed wrapper around a sync.Pool for *bytes.Buffer
type BytesBuffer struct {
	p sync.Pool
}

func NewBytesBuffer() *BytesBuffer {
	return &BytesBuffer{
		p: sync.Pool{
			New: func() interface{} {
				return &bytes.Buffer{}
			},
		},
	}
}

func (p *BytesBuffer) Get() *bytes.Buffer {
	buffer := p.p.Get().(*bytes.Buffer)
	buffer.Reset()
	return buffer
}

func (p *BytesBuffer) Put(b *bytes.Buffer) {
	p.p.Put(b)
}
