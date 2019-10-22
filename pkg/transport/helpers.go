package transport

import (
	"bytes"
	"compress/zlib"
	"context"
	"io"
	"io/ioutil"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/tilinna/clock"
)

var jsonConfig = jsoniter.Config{
	EscapeHTML:  false,
	SortMapKeys: false,
}.Froze()

var jsonDebug = jsoniter.Config{
	EscapeHTML:    false,
	SortMapKeys:   true,
	IndentionStep: 4,
}.Froze()

// interruptableSleep will sleep for the specified duration, or until the context is
// cancelled, whichever comes first.  Returns true if the sleep completes, false if
// the context is canceled.
func interruptableSleep(ctx context.Context, d time.Duration) bool {
	timer := clock.NewTimer(ctx, d)
	select {
	case <-ctx.Done():
		timer.Stop()
		return false
	case <-timer.C:
		return true
	}
}

// consumeAndClose will read all the data from the provided io.ReadCloser, then close
// it.  Intended to safely drain HTTP connections.
func consumeAndClose(r io.ReadCloser) {
	_, _ = io.Copy(ioutil.Discard, r)
	_ = r.Close()
}

func compress(raw []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	compressor, err := zlib.NewWriterLevel(buf, zlib.BestCompression)
	if err != nil {
		return nil, err
	}

	_, _ = compressor.Write(raw) // error is propagated through Close
	err = compressor.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func marshalJson(data interface{}, debug bool) ([]byte, error) {
	buf := &bytes.Buffer{}
	var json jsoniter.API
	if debug {
		json = jsonDebug
	} else {
		json = jsonConfig
	}
	stream := json.BorrowStream(buf)
	stream.WriteVal(data)
	err := stream.Flush()
	if err != nil {
		return nil, err
	}
	json.ReturnStream(stream)
	return buf.Bytes(), nil
}
