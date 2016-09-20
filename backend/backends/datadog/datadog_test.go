package datadog

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/atlassian/gostatsd/types"
	"github.com/stretchr/testify/assert"
)

func TestRetries(t *testing.T) {
	assert := assert.New(t)
	var requestNum uint32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/series", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		n := atomic.AddUint32(&requestNum, 1)
		data, err := ioutil.ReadAll(r.Body)
		if !assert.Nil(err) {
			return
		}
		assert.NotEmpty(data)
		if n == 1 {
			// Return error on first request to trigger a retry
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client, err := NewClient(ts.URL, "apiKey123", defaultMetricsPerBatch, 1*time.Second, 2*time.Second)
	if !assert.Nil(err) {
		return
	}
	res := make(chan []error, 1)
	client.SendMetricsAsync(context.Background(), twoCounters(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.Nil(err)
	}
	assert.EqualValues(2, requestNum)
}

func TestSendMetricsInMultipleBatches(t *testing.T) {
	assert := assert.New(t)
	var requestNum uint32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/series", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		atomic.AddUint32(&requestNum, 1)
		data, err := ioutil.ReadAll(r.Body)
		if !assert.Nil(err) {
			return
		}
		assert.NotEmpty(data)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client, err := NewClient(ts.URL, "apiKey123", 1, 1*time.Second, 2*time.Second)
	if !assert.Nil(err) {
		return
	}
	res := make(chan []error, 1)
	client.SendMetricsAsync(context.Background(), twoCounters(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.Nil(err)
	}
	assert.EqualValues(2, requestNum)
}

func TestSendMetrics(t *testing.T) {
	assert := assert.New(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/series", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		data, err := ioutil.ReadAll(r.Body)
		if !assert.Nil(err) {
			return
		}
		expected := `{"series":[` +
			`{"host":"h1","interval":1.1,"metric":"c1","points":[[100,1.1]],"tags":["tag1"],"type":"rate"},` +
			`{"host":"h1","interval":1.1,"metric":"c1.count","points":[[100,5]],"tags":["tag1"],"type":"gauge"},` +
			`{"host":"h2","interval":1.1,"metric":"t1.lower","points":[[100,0]],"tags":["tag2"],"type":"gauge"},` +
			`{"host":"h2","interval":1.1,"metric":"t1.upper","points":[[100,1]],"tags":["tag2"],"type":"gauge"},` +
			`{"host":"h2","interval":1.1,"metric":"t1.count","points":[[100,1]],"tags":["tag2"],"type":"gauge"},` +
			`{"host":"h2","interval":1.1,"metric":"t1.count_ps","points":[[100,1.1]],"tags":["tag2"],"type":"rate"},` +
			`{"host":"h2","interval":1.1,"metric":"t1.mean","points":[[100,0.5]],"tags":["tag2"],"type":"gauge"},` +
			`{"host":"h2","interval":1.1,"metric":"t1.median","points":[[100,0.5]],"tags":["tag2"],"type":"gauge"},` +
			`{"host":"h2","interval":1.1,"metric":"t1.std","points":[[100,0.1]],"tags":["tag2"],"type":"gauge"},` +
			`{"host":"h2","interval":1.1,"metric":"t1.sum","points":[[100,1]],"tags":["tag2"],"type":"gauge"},` +
			`{"host":"h2","interval":1.1,"metric":"t1.sum_squares","points":[[100,1]],"tags":["tag2"],"type":"gauge"},` +
			`{"host":"h2","interval":1.1,"metric":"t1.count_90","points":[[100,0.1]],"tags":["tag2"],"type":"gauge"},` +
			`{"host":"h3","interval":1.1,"metric":"g1","points":[[100,3]],"tags":["tag3"],"type":"gauge"},` +
			`{"host":"h4","interval":1.1,"metric":"users","points":[[100,3]],"tags":["tag4"],"type":"gauge"}]}`
		assert.Equal([]byte(expected), data)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli, err := NewClient(ts.URL, "apiKey123", 1000, 1*time.Second, 2*time.Second)
	if !assert.Nil(err) {
		return
	}
	cli.(*client).now = func() time.Time {
		return time.Unix(100, 0)
	}
	res := make(chan []error, 1)
	cli.SendMetricsAsync(context.Background(), metricsOneOfEach(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.Nil(err)
	}
}

// twoCounters returns two counters.
func twoCounters() *types.MetricMap {
	return &types.MetricMap{
		MetricStats: types.MetricStats{
			NumStats: 2,
		},
		Counters: types.Counters{
			"stat1": map[string]types.Counter{
				"tag1": types.NewCounter(types.Nanotime(time.Now().UnixNano()), 5, "", nil),
			},
			"stat2": map[string]types.Counter{
				"tag2": types.NewCounter(types.Nanotime(time.Now().UnixNano()), 50, "", nil),
			},
		},
	}
}

func metricsOneOfEach() *types.MetricMap {
	return &types.MetricMap{
		MetricStats: types.MetricStats{
			NumStats:       4,
			ProcessingTime: 10 * time.Millisecond,
		},
		FlushInterval: 1100 * time.Millisecond,
		Counters: types.Counters{
			"c1": map[string]types.Counter{
				"tag1": {PerSecond: 1.1, Value: 5, Timestamp: types.Nanotime(100), Hostname: "h1", Tags: types.Tags{"tag1"}},
			},
		},
		Timers: types.Timers{
			"t1": map[string]types.Timer{
				"tag2": {
					Count:      1,
					PerSecond:  1.1,
					Mean:       0.5,
					Median:     0.5,
					Min:        0,
					Max:        1,
					StdDev:     0.1,
					Sum:        1,
					SumSquares: 1,
					Values:     []float64{0, 1},
					Percentiles: types.Percentiles{
						types.Percentile{Float: 0.1, Str: "count_90"},
					},
					Timestamp: types.Nanotime(200),
					Hostname:  "h2",
					Tags:      types.Tags{"tag2"},
				},
			},
		},
		Gauges: types.Gauges{
			"g1": map[string]types.Gauge{
				"tag3": {Value: 3, Timestamp: types.Nanotime(300), Hostname: "h3", Tags: types.Tags{"tag3"}},
			},
		},
		Sets: types.Sets{
			"users": map[string]types.Set{
				"tag4": {
					Values: map[string]struct{}{
						"joe":  {},
						"bob":  {},
						"john": {},
					},
					Timestamp: types.Nanotime(400),
					Hostname:  "h4",
					Tags:      types.Tags{"tag4"},
				},
			},
		},
	}
}
