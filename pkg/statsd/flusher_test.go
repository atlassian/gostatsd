package statsd

import (
	"errors"
	"testing"
	"strconv"

	"github.com/atlassian/gostatsd"
)

func TestFlusherHandleSendResultNoErrors(t *testing.T) {
	t.Parallel()
	input := [][]error{
		nil,
		make([]error, 0, 2),
		{nil},
	}
	for pos, errs := range input {
		errs := errs
		t.Run(strconv.Itoa(pos), func(t *testing.T) {
			t.Parallel()
			fl := NewMetricFlusher(0, nil, nil, nil, nil, gostatsd.UnknownIP, "host")
			fl.handleSendResult(errs)

			if fl.lastFlush == 0 || fl.lastFlushError != 0 {
				t.Errorf("lastFlush = %d, lastFlushError = %d", fl.lastFlush, fl.lastFlushError)
			}
		})
	}
}

func TestFlusherHandleSendResultError(t *testing.T) {
	t.Parallel()
	input := [][]error{
		{errors.New("boom")},
		{nil, errors.New("boom")},
		{errors.New("boom"), nil},
		{errors.New("boom"), errors.New("boom")},
	}
	for pos, errs := range input {
		errs := errs
		t.Run(strconv.Itoa(pos), func(t *testing.T) {
			t.Parallel()
			fl := NewMetricFlusher(0, nil, nil, nil, nil, gostatsd.UnknownIP, "host")
			fl.handleSendResult(errs)

			if fl.lastFlushError == 0 || fl.lastFlush != 0 {
				t.Errorf("lastFlush = %d, lastFlushError = %d", fl.lastFlush, fl.lastFlushError)
			}
		})
	}
}
