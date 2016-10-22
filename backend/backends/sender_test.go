package backends

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSend(t *testing.T) {
	dc := dummyConn{}
	sender := Sender{
		ConnFactory: func() (net.Conn, error) {
			return &dc, nil
		},
		Sink: make(chan Stream),
		BufPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := sender.Run(ctx); err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			t.Error(err)
		}
	}()
	var wgTest sync.WaitGroup
	for i := 0; i <= 4; i++ {
		wgTest.Add(1)
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			sink := make(chan *bytes.Buffer, i)
			sender.Sink <- Stream{
				Cb: func(errs []error) {
					defer wgTest.Done()
					for _, e := range errs {
						assert.NoError(t, e)
					}
				},
				Buf: sink,
			}
			for x := 0; x < i; x++ {
				sink <- bytes.NewBuffer([]byte{byte(x)})
			}
			close(sink)
		})
	}
	wgTest.Wait()
	assert.Equal(t, []byte{0x0, 0x0, 0x1, 0x0, 0x1, 0x2, 0x0, 0x1, 0x2, 0x3}, dc.buf.Bytes())
}

type dummyConn struct {
	buf      bytes.Buffer
	isClosed bool
}

func (c *dummyConn) Read(b []byte) (int, error) {
	return 0, errors.New("asdasd")
}

func (c *dummyConn) Write(b []byte) (int, error) {
	if c.isClosed {
		panic("closed")
	}
	return c.buf.Write(b)
}

func (c *dummyConn) Close() error {
	c.isClosed = true
	return nil
}

func (c *dummyConn) LocalAddr() net.Addr {
	return nil
}

func (c *dummyConn) RemoteAddr() net.Addr {
	return nil
}

func (c *dummyConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *dummyConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *dummyConn) SetWriteDeadline(t time.Time) error {
	return nil
}
