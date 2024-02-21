package fixtures

import (
	"io"
	"testing"

	"github.com/sirupsen/logrus"
)

type writer struct {
	tb testing.TB
}

var _ io.Writer = (*writer)(nil)

func (w writer) Write(p []byte) (int, error) {
	w.tb.Log(string(p))
	return len(p), nil
}

func NewTestLogger(tb testing.TB, opts ...func(logrus.FieldLogger)) logrus.FieldLogger {
	l := logrus.New()

	for _, opt := range opts {
		opt(l)
	}
	l.SetOutput(writer{tb: tb})

	return l
}
