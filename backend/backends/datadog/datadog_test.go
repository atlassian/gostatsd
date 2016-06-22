package datadog

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/atlassian/gostatsd/types"

	"golang.org/x/net/context"
)

func TestRetries(t *testing.T) {
	var requestNum uint32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/series", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		atomic.AddUint32(&requestNum, 1)
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		if len(data) == 0 {
			t.Errorf("empty body")
		}
		if r.ContentLength != int64(len(data)) {
			t.Errorf("unexpected body length: %d. Content-Length is %d", len(data), r.ContentLength)
		}
		if requestNum == 1 {
			// Return error on first request to trigger a retry
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client, err := NewClient(ts.URL, "apiKey123", defaultMetricsPerBatch, 1*time.Second, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	res := make(chan []error, 1)
	client.SendMetricsAsync(context.Background(), metrics(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	if requestNum != 2 {
		t.Errorf("unexpected number of requests: %d", requestNum)
	}
}

func TestSendMetrics(t *testing.T) {
	var requestNum uint32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/series", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		atomic.AddUint32(&requestNum, 1)
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		if len(data) == 0 {
			t.Errorf("empty body")
			w.WriteHeader(http.StatusBadRequest)
		}
		if r.ContentLength != int64(len(data)) {
			t.Errorf("unexpected body length: %d. Content-Length is %d", len(data), r.ContentLength)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client, err := NewClient(ts.URL, "apiKey123", 1, 1*time.Second, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	res := make(chan []error, 1)
	client.SendMetricsAsync(context.Background(), metrics(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	if requestNum != 2 {
		t.Errorf("unexpected number of requests: %d", requestNum)
	}
}

func metrics() *types.MetricMap {
	return &types.MetricMap{
		NumStats: 2,
		Counters: types.Counters{
			"stat1": map[string]types.Counter{
				"tag1": types.NewCounter(time.Now(), 1*time.Second, 5),
			},
			"stat2": map[string]types.Counter{
				"tag2": types.NewCounter(time.Now(), 2*time.Second, 50),
			},
		},
	}
}
