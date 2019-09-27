package transport

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func poolFromConfig(t *testing.T, config string) *TransportPool {
	v := viper.New()
	v.SetConfigType("toml")
	err := v.ReadConfig(bytes.NewBufferString(config))
	require.NoError(t, err)
	p := NewTransportPool(logrus.New(), v)
	return p
}

func TestClientBasicPost(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	var wg sync.WaitGroup
	wg.Add(1)
	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()

		// Remove the headers added by the library
		r.Header.Del("accept-encoding")
		r.Header.Del("content-length")

		expected := http.Header{}
		expected.Set("content-type", "text/plain")
		expected.Set("content-encoding", "identity")
		expected.Set("user-agent", "gostatsd")
		require.EqualValues(t, expected, r.Header)

		data, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())
		require.EqualValues(t, "body", string(data))
		w.WriteHeader(200)
	})
	ts := httptest.NewServer(mux)

	p := poolFromConfig(t, "")
	c, err := p.Get("default")
	require.NoError(t, err)
	c.PostRaw(context.Background(), ts.URL+"/test", "text/plain", "identity", nil, []byte("body"))
	wg.Wait()
}

func TestClientCallerHeaders(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	var wg sync.WaitGroup
	wg.Add(1)
	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		// Remove the headers added by the library
		r.Header.Del("accept-encoding")
		r.Header.Del("content-length")

		expected := http.Header{}
		expected.Set("content-type", "text/plain")
		expected.Set("content-encoding", "identity")
		expected.Set("user-agent", "custom") // caller override
		expected.Set("key", "value")         // caller added
		require.EqualValues(t, expected, r.Header)

		consumeAndClose(r.Body)
		w.WriteHeader(200)
	})
	ts := httptest.NewServer(mux)

	p := poolFromConfig(t, "")
	c, err := p.Get("default")
	require.NoError(t, err)
	headers := map[string]string{
		"key":        "value",
		"user-agent": "custom",
	}
	c.PostRaw(context.Background(), ts.URL+"/test", "text/plain", "identity", headers, nil)
	wg.Wait()
}

func TestClientUserAddHeaders(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	var wg sync.WaitGroup
	wg.Add(1)
	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		// Remove the headers added by the library
		r.Header.Del("accept-encoding")
		r.Header.Del("content-length")

		expected := http.Header{}
		expected.Set("content-type", "text/plain")
		expected.Set("content-encoding", "identity")
		// key1 not present, removed by user
		expected.Set("key2", "value2")       // caller added
		expected.Set("user-agent", "custom") // caller added + user override
		expected.Set("foo", "bar")           // user added
		require.EqualValues(t, expected, r.Header)

		consumeAndClose(r.Body)
		w.WriteHeader(200)
	})
	ts := httptest.NewServer(mux)

	p := poolFromConfig(t, `
[transport.default]
custom-headers = {"foo"="bar", "key1"=""}
`)
	c, err := p.Get("default")
	require.NoError(t, err)
	headers := map[string]string{
		"key1":       "value1",
		"key2":       "value2",
		"user-agent": "custom",
	}
	c.PostRaw(context.Background(), ts.URL+"/test", "text/plain", "identity", headers, nil)
	wg.Wait()
}

func TestClientRetries(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	var wg sync.WaitGroup
	wg.Add(1)
	count := uint64(0)
	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		consumeAndClose(r.Body)
		if atomic.AddUint64(&count, 1) == 2 {
			w.WriteHeader(200)
			defer wg.Done()
		} else {
			w.WriteHeader(500)
		}
	})
	ts := httptest.NewServer(mux)

	p := poolFromConfig(t, `
[transport.default]
retry-policy='constant'
retry-interval='1ns'
`)
	c, err := p.Get("default")
	require.NoError(t, err)
	c.PostRaw(context.Background(), ts.URL+"/test", "text/plain", "identity", nil, nil)
	wg.Wait()
}
