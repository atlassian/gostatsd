package datadogexp

import (
	"net/http"
	"net/http/httptrace"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

type tracer struct {
	count  uint64
	logger logrus.FieldLogger
	trace  *httptrace.ClientTrace
}

const uMinus1 = ^uint64(0)

func newTracer(logger logrus.FieldLogger) *tracer {
	t := &tracer{
		logger: logger,
	}
	t.trace = &httptrace.ClientTrace{
		GotConn:     t.traceGotConn,
		PutIdleConn: t.tracePutIdleConn,
	}
	return t
}

func (t *tracer) traceGotConn(gci httptrace.GotConnInfo) {
	if gci.WasIdle {
		c := atomic.AddUint64(&t.count, uMinus1)
		t.logger.WithField("count", c).Info("Borrowed")
	} else {
		t.logger.Info("Created")
	}
}

func (t *tracer) tracePutIdleConn(err error) {
	if err != nil {
		t.logger.Info("Failed to put")
	} else {
		c := atomic.AddUint64(&t.count, 1)
		t.logger.WithField("count", c).Info("Returned")
	}
}

func (t *tracer) WithTrace(r *http.Request) *http.Request {
	return r.WithContext(httptrace.WithClientTrace(r.Context(), t.trace))
}
