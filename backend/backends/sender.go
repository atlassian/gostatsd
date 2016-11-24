package backends

import (
	"bytes"
	"context"
	"net"
	"sync"
	"time"

	"github.com/atlassian/gostatsd"

	log "github.com/Sirupsen/logrus"
)

const maxStreamsPerConnection = 100

type ConnFactory func() (net.Conn, error)

type Stream struct {
	Cb  gostatsd.SendCallback
	Buf chan *bytes.Buffer
}

type Sender struct {
	ConnFactory  ConnFactory
	Sink         chan Stream
	BufPool      sync.Pool
	WriteTimeout time.Duration
}

func (s *Sender) Run(ctx context.Context) error {
	var stream *Stream
	var errs []error
	for {
		w, err := s.ConnFactory()
		if err != nil {
			log.Warnf("Failed to connect: %v", err)
			// TODO do backoff
			timer := time.NewTimer(1 * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if stream, errs, err = s.innerRun(ctx, w, stream, errs); err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				return err
			}
			errs = append(errs, err)
		}
	}
}

func (s *Sender) innerRun(ctx context.Context, conn net.Conn, stream *Stream, errs []error) (*Stream, []error, error) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Warnf("Close failed: %v", err)
		}
	}()
	var err error
loop:
	for streamCount := 0; streamCount < maxStreamsPerConnection; streamCount++ {
		if stream == nil {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				break loop
			case s := <-s.Sink:
				stream = &s
			}
		}
		for buf := range stream.Buf {
			if s.WriteTimeout > 0 {
				if e := conn.SetWriteDeadline(time.Now().Add(s.WriteTimeout)); e != nil {
					log.Warnf("Failed to set write deadline: %v", e)
				}
			}
			_, err = conn.Write(buf.Bytes())
			s.PutBuffer(buf)
			if err != nil {
				break loop
			}
		}
		stream.Cb(errs)
		stream = nil
		errs = nil
	}
	return stream, errs, err
}

func (s *Sender) GetBuffer() *bytes.Buffer {
	return s.BufPool.Get().(*bytes.Buffer)
}

func (s *Sender) PutBuffer(buf *bytes.Buffer) {
	buf.Reset() // Reset buffer before returning it into the pool
	s.BufPool.Put(buf)
}
