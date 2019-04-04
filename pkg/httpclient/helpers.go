package httpclient

import (
	"bytes"
	"compress/zlib"
	"context"
	"io"
	"io/ioutil"
	"time"

	"github.com/spf13/viper"

	"github.com/tilinna/clock"
)

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

// interruptableSleep will sleep for the specified duration,
// or until the context is cancelled, whichever comes first.
func interruptableSleep(ctx context.Context, d time.Duration) bool {
	timer := clock.NewTimer(ctx, d)
	select {
	case <-ctx.Done():
		timer.Stop()
		return true
	case <-timer.C:
		return false
	}
}

func consumeAndClose(r io.ReadCloser) {
	_, _ = io.Copy(ioutil.Discard, r)
	_ = r.Close()
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	return n
}
