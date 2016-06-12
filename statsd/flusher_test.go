package statsd

import (
	"errors"
	"testing"
)

func TestFlusherHandleSendResultNoErrors(t *testing.T) {
	input := [][]error{
		nil,
		make([]error, 0, 2),
		{nil},
	}
	for pos, errs := range input {
		fl := NewFlusher(0, nil, nil, nil, nil).(*flusher)
		fl.handleSendResult(errs)

		if fl.lastFlush == 0 || fl.lastFlushError != 0 {
			t.Errorf("%d lastFlush = %d, lastFlushError = %d", pos, fl.lastFlush, fl.lastFlushError)
		}

	}
}

func TestFlusherHandleSendResultError(t *testing.T) {
	input := [][]error{
		{errors.New("boom")},
		{nil, errors.New("boom")},
		{errors.New("boom"), nil},
		{errors.New("boom"), errors.New("boom")},
	}
	for pos, errs := range input {
		fl := NewFlusher(0, nil, nil, nil, nil).(*flusher)
		fl.handleSendResult(errs)

		if fl.lastFlushError == 0 || fl.lastFlush != 0 {
			t.Errorf("%d lastFlush = %d, lastFlushError = %d", pos, fl.lastFlush, fl.lastFlushError)
		}

	}
}
