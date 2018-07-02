package datadogexp

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func newTestBodyWriter(ctx context.Context, t *testing.T, mux *http.ServeMux) (*bodyWriter, *httptest.Server, chan<- *renderedBody) {
	ts := httptest.NewServer(mux)
	renderBody := make(chan *renderedBody)

	client := &http.Client{}

	logger := logrus.New()

	bw, err := newBodyWriter(
		logger,
		"gostatsd",
		ts.URL,
		"1234",
		client,
		func() backoff.BackOff {
			return backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 3)
		},
		renderBody,
	)
	require.NoError(t, err, "failed to create bodyWriter")

	go bw.Run(ctx)

	return bw, ts, renderBody
}

func TestBodyWriterRetries(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	calls := int64(0)

	done := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/series", func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)

		require.NoError(t, err, "error reading body")
		_ = r.Body.Close()

		require.Equal(t, "body", string(body), "unexpected body")

		if atomic.AddInt64(&calls, 1) == 3 {
			w.WriteHeader(http.StatusAccepted)
			select {
			case <-ctx.Done():
				require.Fail(t, "timeout submitting done")
			case done <- struct{}{}:
			}
		} else {
			w.WriteHeader(http.StatusForbidden)
		}
		logrus.Info("Request")
	})

	_, ts, rb := newTestBodyWriter(ctx, t, mux)
	defer ts.Close()

	b := &renderedBody{
		[]byte("body"),
		false,
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout submitting body")
	case rb <- b:
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout waiting for done")
	case <-done:
	}

	require.EqualValues(t, 3, calls, "unexpected retry count")
}

func TestBodyWriterSetsCompressionHeader(t *testing.T) {
	// Does not test for actual compressed content, as it is not responsible for compressing.
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	done := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/series", func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err, "error reading body")
		_ = r.Body.Close()

		require.Equal(t, "body", string(body), "unexpected body")
		require.Equal(t, "deflate", r.Header.Get("Content-Encoding"), "invalid encoding")
		w.WriteHeader(http.StatusAccepted)

		select {
		case <-ctx.Done():
			require.Fail(t, "timeout submitting done")
		case done <- struct{}{}:
		}
	})

	_, ts, rb := newTestBodyWriter(ctx, t, mux)
	defer ts.Close()

	b := &renderedBody{
		[]byte("body"),
		true,
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout submitting body")
	case rb <- b:
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout waiting for done")
	case <-done:
	}
}

func TestBodyWriterSkipsCompressionHeader(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	done := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/series", func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err, "error reading body")
		_ = r.Body.Close()

		require.Equal(t, "body", string(body), "unexpected body")
		require.Equal(t, "", r.Header.Get("Content-Encoding"), "invalid encoding")
		w.WriteHeader(http.StatusAccepted)

		select {
		case <-ctx.Done():
			require.Fail(t, "timeout submitting done")
		case done <- struct{}{}:
		}
	})

	_, ts, rb := newTestBodyWriter(ctx, t, mux)
	defer ts.Close()

	b := &renderedBody{
		[]byte("body"),
		false,
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout submitting body")
	case rb <- b:
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout waiting for done")
	case <-done:
	}
}

func TestBodyWriterSetsCommonHeaders(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	done := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/series", func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err, "error reading body")
		_ = r.Body.Close()

		require.Equal(t, "body", string(body), "unexpected body")
		require.Equal(t, "application/json", r.Header.Get("Content-Type"), "invalid content-type")
		w.WriteHeader(http.StatusAccepted)

		select {
		case <-ctx.Done():
			require.Fail(t, "timeout submitting done")
		case done <- struct{}{}:
		}
	})

	_, ts, rb := newTestBodyWriter(ctx, t, mux)
	defer ts.Close()

	b := &renderedBody{
		[]byte("body"),
		false,
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout submitting body")
	case rb <- b:
	}

	select {
	case <-ctx.Done():
		require.Fail(t, "timeout waiting for done")
	case <-done:
	}
}
