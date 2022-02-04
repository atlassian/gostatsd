package newrelic

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"index/suffixarray"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tilinna/clock"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/transport"
)

func advanceTime(c *clock.Mock, ch <-chan struct{}) {
	for {
		select {
		case <-ch:
			return
		default:
			c.AddNext()
		}
	}
}

func TestRetries(t *testing.T) {
	t.Parallel()
	var requestNum uint32
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/data", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		n := atomic.AddUint32(&requestNum, 1)
		data, err := ioutil.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return
		}
		assert.NotEmpty(t, data)
		if n == 1 {
			// Return error on first request to trigger a retry
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	v := viper.New()
	v.SetDefault("transport.default.client-timeout", 1*time.Second)
	p := transport.NewTransportPool(logrus.New(), v)

	client, err := NewClient("default", ts.URL+"/v1/data", "", "GoStatsD", "", "", "", "metric_name", "metric_type",
		"metric_per_second", "metric_value", "samples_min", "samples_max", "samples_count",
		"samples_mean", "samples_median", "samples_std_dev", "samples_sum", "samples_sum_squares", "agent",
		defaultMetricsPerBatch, defaultMaxRequests, 2*time.Second, 1*time.Second, gostatsd.TimerSubtypes{}, logrus.New(), p)

	require.NoError(t, err)
	res := make(chan []error, 1)
	clck := clock.NewMock(time.Unix(0, 0))
	ctx := clock.Context(context.Background(), clck)
	ch := make(chan struct{})
	go advanceTime(clck, ch)
	client.SendMetricsAsync(ctx, twoCounters(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.NoError(t, err)
	}
	assert.EqualValues(t, 2, requestNum)
	ch <- struct{}{}
}

func TestRetryAfter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		retryAfterHeader      string
		maxRequestElapsedTime time.Duration
		expectedDuration      int64
	}{
		{
			name:                  "Positive valid RetryAfter less than maxRequestElapsedTime",
			retryAfterHeader:      "42",
			maxRequestElapsedTime: 60 * time.Second,
			expectedDuration:      42,
		},
		{
			name:                  "Positive valid RetryAfter greather than maxRequestElapsedTime",
			retryAfterHeader:      "42",
			maxRequestElapsedTime: 2 * time.Second,
			expectedDuration:      2,
		},
		{
			name:                  "Negative invalid RetryAfter",
			retryAfterHeader:      "-10",
			maxRequestElapsedTime: 2 * time.Second,
			expectedDuration:      0,
		},
		{
			name:                  "Non-numeric invalid RetryAfter",
			retryAfterHeader:      "wrong",
			maxRequestElapsedTime: 2 * time.Second,
			expectedDuration:      0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var requestNum uint32
			var requestTimes []int64

			mux := http.NewServeMux()
			clck := clock.NewMock(time.Unix(0, 0))

			mux.HandleFunc("/v1/data", func(w http.ResponseWriter, r *http.Request) {
				requestTimes = append(requestTimes, clck.Now().Unix())
				defer r.Body.Close()
				n := atomic.AddUint32(&requestNum, 1)
				data, err := ioutil.ReadAll(r.Body)

				assert.NoError(t, err)
				assert.NotEmpty(t, data)
				if n == 1 {
					// Return error on first request to trigger a retry after a second
					w.Header().Add("Retry-After", tt.retryAfterHeader)
					w.WriteHeader(http.StatusTooManyRequests)
				}
			})
			ts := httptest.NewServer(mux)
			defer ts.Close()

			v := viper.New()
			v.SetDefault("transport.default.client-timeout", 1*time.Second)
			p := transport.NewTransportPool(logrus.New(), v)

			client, err := NewClient("default", ts.URL+"/v1/data", "", "GoStatsD", "", "", "", "metric_name", "metric_type",
				"metric_per_second", "metric_value", "samples_min", "samples_max", "samples_count",
				"samples_mean", "samples_median", "samples_std_dev", "samples_sum", "samples_sum_squares", "agent",
				defaultMetricsPerBatch, defaultMaxRequests, tt.maxRequestElapsedTime, 1*time.Second, gostatsd.TimerSubtypes{}, logrus.New(), p)

			require.NoError(t, err)
			res := make(chan []error, 1)
			ctx := clock.Context(context.Background(), clck)
			ch := make(chan struct{})
			go advanceTime(clck, ch)
			client.SendMetricsAsync(ctx, twoCounters(), func(errs []error) {
				res <- errs
			})
			errs := <-res
			for _, err := range errs {
				assert.NoError(t, err)
			}
			assert.EqualValues(t, 2, requestNum)
			assert.Len(t, requestTimes, 2)
			assert.Equal(t, int64(tt.expectedDuration), requestTimes[1]-requestTimes[0])
			ch <- struct{}{}
		})
	}
}

func TestSendMetricsInMultipleBatches(t *testing.T) {
	t.Parallel()
	var requestNum uint32
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/data", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		atomic.AddUint32(&requestNum, 1)
		data, err := ioutil.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return
		}
		assert.NotEmpty(t, data)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	v := viper.New()
	v.SetDefault("transport.default.client-timeout", 1*time.Second)
	p := transport.NewTransportPool(logrus.New(), v)

	client, err := NewClient("default", ts.URL+"/v1/data", "", "GoStatsD", "", "", "", "metric_name", "metric_type",
		"metric_per_second", "metric_value", "samples_min", "samples_max", "samples_count",
		"samples_mean", "samples_median", "samples_std_dev", "samples_sum", "samples_sum_squares", "agent",
		1, defaultMaxRequests, 2*time.Second, 1*time.Second, gostatsd.TimerSubtypes{}, logrus.New(), p)
	require.NoError(t, err)
	res := make(chan []error, 1)
	client.SendMetricsAsync(context.Background(), twoCounters(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.NoError(t, err)
	}
	assert.EqualValues(t, 2, requestNum)
}

func TestSendMetrics(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		expected  string
		flushType string
		apiKey    string
	}{
		{
			name:      "infra",
			flushType: "infra",
			apiKey:    "",
			expected: `{"name":"com.newrelic.gostatsd","protocol_version":"2","integration_version":"2.3.1","data":[{"metrics":` +
				`[{"event_type":"GoStatsD","integration_version":"2.3.1","interval":1,"metric_name":"g1","metric_type":"gauge","metric_value":3,"tag3":"true","timestamp":100},` +
				`{"event_type":"GoStatsD","integration_version":"2.3.1","interval":1,"metric_name":"c1","metric_per_second":1.1,"metric_type":"counter","metric_value":5,"tag1":"true","timestamp":100},` +
				`{"event_type":"GoStatsD","integration_version":"2.3.1","interval":1,"metric_name":"users","metric_type":"set","metric_value":3,"tag4":"true","timestamp":100},` +
				`{"count_90":0.1,"event_type":"GoStatsD","integration_version":"2.3.1","interval":1,"metric_name":"t1","metric_per_second":1.1,"metric_type":"timer","metric_value":1,` +
				`"samples_count":1,"samples_max":1,"samples_mean":0.5,"samples_median":0.5,"samples_min":0,"samples_std_dev":0.1,"samples_sum":1,"samples_sum_squares":1,"tag2":"true","timestamp":100}]}]}`,
		},
		{
			name:      "insights",
			flushType: "insights",
			apiKey:    "some-api-key",
			expected: `[{"eventType":"GoStatsD","integration_version":"2.3.1","interval":1,"metric_name":"g1","metric_type":"gauge","metric_value":3,"tag3":"true","timestamp":100},` +
				`{"eventType":"GoStatsD","integration_version":"2.3.1","interval":1,"metric_name":"c1","metric_per_second":1.1,"metric_type":"counter","metric_value":5,"tag1":"true","timestamp":100},` +
				`{"eventType":"GoStatsD","integration_version":"2.3.1","interval":1,"metric_name":"users","metric_type":"set","metric_value":3,"tag4":"true","timestamp":100},` +
				`{"count_90":0.1,"eventType":"GoStatsD","integration_version":"2.3.1","interval":1,"metric_name":"t1","metric_per_second":1.1,"metric_type":"timer","metric_value":1,"samples_count":1,` +
				`"samples_max":1,"samples_mean":0.5,"samples_median":0.5,"samples_min":0,"samples_std_dev":0.1,"samples_sum":1,"samples_sum_squares":1,"tag2":"true","timestamp":100}]`,
		},
		{
			name:      "metrics",
			flushType: "metrics",
			apiKey:    "some-api-key",
			expected: `[{"common":{"attributes":{"integration.name":"GoStatsD","integration.version":"2.3.1"},"interval.ms":1000},` +
				`"metrics":[{"name":"g1","value":3,"type":"gauge","timestamp":100,"attributes":{"statsdType":"gauge","tag3":"true"}},` +
				`{"name":"c1.per_second","value":1.1,"type":"gauge","timestamp":100,"attributes":{"statsdType":"gauge","tag1":"true"}},` +
				`{"name":"c1","value":5,"type":"count","timestamp":100,"attributes":{"statsdType":"counter","tag1":"true"}},` +
				`{"name":"users","timestamp":100,"attributes":{"statsdType":"set","tag4":"true"}},{"name":"t1.per_second","value":1.1,"type":"gauge","timestamp":100,"attributes":{"statsdType":"gauge","tag2":"true"}},` +
				`{"name":"t1.mean","value":0.5,"type":"gauge","timestamp":100,"attributes":{"statsdType":"gauge","tag2":"true"}},` +
				`{"name":"t1.median","value":0.5,"type":"gauge","timestamp":100,"attributes":{"statsdType":"gauge","tag2":"true"}},` +
				`{"name":"t1.std_dev","value":0.1,"type":"gauge","timestamp":100,"attributes":{"statsdType":"gauge","tag2":"true"}},` +
				`{"name":"t1.sum_squares","value":1,"type":"gauge","timestamp":100,"attributes":{"statsdType":"gauge","tag2":"true"}},` +
				`{"name":"t1.count.percentiles","value":0.1,"type":"gauge","timestamp":100,"attributes":{"percentile":90,"statsdType":"gauge","tag2":"true"}},` +
				`{"name":"t1.summary","value":{"count":1,"max":1,"min":0,"sum":1},"type":"summary","timestamp":100,"attributes":{"statsdType":"timer","tag2":"true"}}]}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			mux := http.NewServeMux()
			path := "/v1/data"
			if tt.flushType == "metrics" {
				path = "/metric/v1"
			}
			mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
				enc := r.Header.Get("Content-Encoding")
				var body string
				body, done := decodeBody(enc, r, t)
				if done {
					return
				}
				assert.Equal(t, tt.expected, body)
				assert.Equal(t, tt.apiKey, r.Header.Get("X-Insert-Key"))
			})
			ts := httptest.NewServer(mux)
			defer ts.Close()

			v := viper.New()
			v.SetDefault("transport.default.client-timeout", 1*time.Second)
			p := transport.NewTransportPool(logrus.New(), v)

			client, err := NewClient("default", ts.URL+"/v1/data", ts.URL+"/metric/v1", "GoStatsD", tt.flushType, tt.apiKey, "", "metric_name", "metric_type",
				"metric_per_second", "metric_value", "samples_min", "samples_max", "samples_count",
				"samples_mean", "samples_median", "samples_std_dev", "samples_sum", "samples_sum_squares", "agent",
				defaultMetricsPerBatch, defaultMaxRequests, 2*time.Second, 1*time.Second, gostatsd.TimerSubtypes{}, logrus.New(), p)

			require.NoError(t, err)
			res := make(chan []error, 1)
			ctx := clock.Context(context.Background(), clock.NewMock(time.Unix(100, 0)))
			client.SendMetricsAsync(ctx, metricsOneOfEach(), func(errs []error) {
				res <- errs
			})
			errs := <-res
			for _, err := range errs {
				assert.NoError(t, err)
			}
		})
	}

}

func decodeBody(enc string, r *http.Request, t *testing.T) (string, bool) {
	body := ""
	if enc == "gzip" {
		gr, err := gzip.NewReader(r.Body)
		if !assert.NoError(t, err) {
			t.FailNow()
			return "", true
		}
		data, err := ioutil.ReadAll(gr)
		if !assert.NoError(t, err) {
			return "", true
		}
		body = string(data)
	} else {
		data, err := ioutil.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return "", true
		}
		body = string(data)
	}
	return body, false
}

func TestSendMetricsWithHistogram(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/data", func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return
		}

		expected := []string{
			`{"event_type":"GoStatsD","integration_version":"2.3.1","interval":1,"le":20,"metric_name":"t1.histogram","metric_per_second":0,"metric_type":"counter","metric_value":5,"timestamp":100}`,
			`{"event_type":"GoStatsD","integration_version":"2.3.1","interval":1,"le":30,"metric_name":"t1.histogram","metric_per_second":0,"metric_type":"counter","metric_value":10,"timestamp":100}`,
			`{"event_type":"GoStatsD","integration_version":"2.3.1","interval":1,"le":40,"metric_name":"t1.histogram","metric_per_second":0,"metric_type":"counter","metric_value":10,"timestamp":100}`,
			`{"event_type":"GoStatsD","integration_version":"2.3.1","interval":1,"le":50,"metric_name":"t1.histogram","metric_per_second":0,"metric_type":"counter","metric_value":10,"timestamp":100}`,
			`{"event_type":"GoStatsD","integration_version":"2.3.1","interval":1,"le":60,"metric_name":"t1.histogram","metric_per_second":0,"metric_type":"counter","metric_value":19,"timestamp":100}`,
			`{"event_type":"GoStatsD","integration_version":"2.3.1","interval":1,"le":"infinity","metric_name":"t1.histogram","metric_per_second":0,"metric_type":"counter","metric_value":19,"timestamp":100}`,
		}

		for _, e := range expected {
			assert.Contains(t, string(data), e)
		}
		assert.Equal(t, 6, countMatches(string(data), "metric_name"))
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	v := viper.New()
	v.SetDefault("transport.default.client-timeout", 1*time.Second)
	p := transport.NewTransportPool(logrus.New(), v)
	client, err := NewClient("default", ts.URL+"/v1/data", "", "GoStatsD", "", "", "", "metric_name", "metric_type",
		"metric_per_second", "metric_value", "samples_min", "samples_max", "samples_count",
		"samples_mean", "samples_median", "samples_std_dev", "samples_sum", "samples_sum_squares", "agent",
		defaultMetricsPerBatch, defaultMaxRequests, 2*time.Second, 1*time.Second, gostatsd.TimerSubtypes{}, logrus.New(), p)

	require.NoError(t, err)
	res := make(chan []error, 1)
	ctx := clock.Context(context.Background(), clock.NewMock(time.Unix(100, 0)))
	client.SendMetricsAsync(ctx, metricsWithHistogram(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.NoError(t, err)
	}
}

// twoCounters returns two counters.
func twoCounters() *gostatsd.MetricMap {
	return &gostatsd.MetricMap{
		Counters: gostatsd.Counters{
			"stat1": map[string]gostatsd.Counter{
				"tag1": gostatsd.NewCounter(0, 5, "", nil),
			},
			"stat2": map[string]gostatsd.Counter{
				"tag2": gostatsd.NewCounter(0, 50, "", nil),
			},
		},
	}
}

func metricsOneOfEach() *gostatsd.MetricMap {
	return &gostatsd.MetricMap{
		Counters: gostatsd.Counters{
			"c1": map[string]gostatsd.Counter{
				"tag1": {PerSecond: 1.1, Value: 5, Timestamp: 0, Source: "h1", Tags: gostatsd.Tags{"tag1"}},
			},
		},
		Timers: gostatsd.Timers{
			"t1": map[string]gostatsd.Timer{
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
					Percentiles: gostatsd.Percentiles{
						gostatsd.Percentile{Float: 0.1, Str: "count_90"},
					},
					Timestamp: 0,
					Source:    "h2",
					Tags:      gostatsd.Tags{"tag2"},
				},
			},
		},
		Gauges: gostatsd.Gauges{
			"g1": map[string]gostatsd.Gauge{
				"tag3": {Value: 3, Timestamp: 0, Source: "h3", Tags: gostatsd.Tags{"tag3"}},
			},
		},
		Sets: gostatsd.Sets{
			"users": map[string]gostatsd.Set{
				"tag4": {
					Values: map[string]struct{}{
						"joe":  {},
						"bob":  {},
						"john": {},
					},
					Timestamp: 0,
					Source:    "h4",
					Tags:      gostatsd.Tags{"tag4"},
				},
			},
		},
	}
}

func metricsWithHistogram() *gostatsd.MetricMap {
	mm := gostatsd.NewMetricMap()
	mm.Timers["t1"] = map[string]gostatsd.Timer{}
	mm.Timers["t1"]["gsd_histogram:20_30_40_50_60"] = gostatsd.Timer{
		Values:    []float64{10},
		Timestamp: 0,
		Histogram: map[gostatsd.HistogramThreshold]int{
			20:                                       5,
			30:                                       10,
			40:                                       10,
			50:                                       10,
			60:                                       19,
			gostatsd.HistogramThreshold(math.Inf(1)): 19,
		}}
	return mm
}

func TestEventFormatter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		expected string
	}{
		{
			name: "infra",
			expected: `{"name":"com.newrelic.gostatsd","protocol_version":"2","integration_version":"2.3.1","data":` +
				`[{"metrics":[{"AggregationKey":"","AlertType":"","DateHappened":0,"Hostname":"blah","Priority":"low","SourceTypeName":"","Text":"hi","Title":"EventTitle","event_type":"GoStatsD","name":"event",` +
				`"tag_1":"-infinity","tag_2":"infinity","tag_3":"+infinity","tag_4":"NaN"}]}]}`,
		},
		{
			name: "insights",
			expected: `[{"AggregationKey":"","AlertType":"","DateHappened":0,"Hostname":"blah","Priority":"low","SourceTypeName":"","Text":"hi","Title":"EventTitle","eventType":"GoStatsD","name":"event",` +
				`"tag_1":"-infinity","tag_2":"infinity","tag_3":"+infinity","tag_4":"NaN"}]`,
		},
		{
			name: "metrics",
			expected: `[{"AggregationKey":"","AlertType":"","DateHappened":0,"Hostname":"blah","Priority":"low","SourceTypeName":"","Text":"hi","Title":"EventTitle","eventType":"GoStatsD","name":"event",` +
				`"tag_1":"-infinity","tag_2":"infinity","tag_3":"+infinity","tag_4":"NaN"}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := viper.New()
			v.SetDefault("transport.default.client-timeout", 1*time.Second)
			p := transport.NewTransportPool(logrus.New(), v)

			client, err := NewClient("default", "v1/data", "", "GoStatsD", tt.name, "api-key", "", "metric_name", "metric_type",
				"metric_per_second", "metric_value", "samples_min", "samples_max", "samples_count",
				"samples_mean", "samples_median", "samples_std_dev", "samples_sum", "samples_sum_squares", "agent",
				defaultMetricsPerBatch, defaultMaxRequests, 2*time.Second, 1*time.Second, gostatsd.TimerSubtypes{}, logrus.New(), p)
			require.NoError(t, err)

			tags := []string{"tag_1:-infinity", "tag_2:infinity", "tag_3:+infinity", "tag_4:NaN"}
			gostatsdEvent := gostatsd.Event{Title: "EventTitle", Text: "hi", Source: "blah", Priority: 1, Tags: tags}
			formattedEvent := client.EventFormatter(&gostatsdEvent)
			fevent, err := json.Marshal(formattedEvent)
			require.NoError(t, err)
			require.Equal(t, tt.expected, string(fevent))
		})
	}
}

func countMatches(s string, m string) int {
	index := suffixarray.New([]byte(s))
	return len(index.Lookup([]byte(m), -1))
}
