package influxdb

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/fixtures"
	"github.com/atlassian/gostatsd/pkg/transport"
)

func TestNewClientInvalid(t *testing.T) {
	t.Parallel()

	steps := []struct {
		key             string
		value           interface{}
		expectedFailure string
	}{
		// note: these are not parallel tests, they build on each other
		{paramApiVersion, -1, paramApiVersion},
		{paramApiVersion, 1, paramDatabase},
		{paramDatabase, "test-database", paramApiEndpoint},
		{paramApiEndpoint, "\t", "net/url"},
		{paramApiEndpoint, "http://localhost", ""},
		{paramMaxRequests, -5, paramMaxRequests},
		{paramMaxRequests, defaultMaxRequests, ""},
		{paramMaxRequestElapsedTime, -5, paramMaxRequestElapsedTime},
		{paramMaxRequestElapsedTime, defaultMaxRequestElapsedTime, ""},
		{paramMetricsPerBatch, -5, paramMetricsPerBatch},
		{paramMetricsPerBatch, defaultMetricsPerBatch, ""},
	}

	v := viper.New()
	for idx, step := range steps {
		v.Set("influxdb."+step.key, step.value)
		client, err := NewClientFromViper(v, logrus.New(), transport.NewTransportPool(logrus.New(), viper.New()))
		if step.expectedFailure == "" {
			require.NoErrorf(t, err, "step %d, key %s", idx, step.key)
			require.NotNilf(t, client, "step %d, key %s", idx, step.key)
		} else {
			require.Errorf(t, err, "step %d, key %s", idx, step.key)
			require.Containsf(t, err.Error(), step.expectedFailure, "step %d, key %s", idx, step.key)
			require.Nilf(t, client, "step %d, key %s", idx, step.key)
		}
	}
}

func TestNewClientValid(t *testing.T) {
	t.Parallel()
	v := viper.New()
	v.Set("influxdb."+paramApiVersion, 1)
	v.Set("influxdb."+paramDatabase, "test-db")
	v.Set("influxdb."+paramApiEndpoint, "http://localhost")
	client, err := NewClientFromViper(v, logrus.New(), transport.NewTransportPool(logrus.New(), viper.New()))
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestRetries(t *testing.T) {
	t.Parallel()
	var requestNum uint32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/write", func(w http.ResponseWriter, r *http.Request) {
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
	v.Set("transport.default.client-timeout", 1*time.Second)
	p := transport.NewTransportPool(logrus.New(), v)
	cli, err := NewClient(
		ts.URL,
		true,
		"creds",
		defaultMaxRequests,
		defaultMaxRequestElapsedTime,
		defaultMetricsPerBatch,
		"default",
		configV2{
			bucket: "bucket",
			org:    "org",
		},
		gostatsd.TimerSubtypes{},
		logrus.New(),
		p,
	)
	require.NoError(t, err)

	res := make(chan []error, 1)
	ctx, cancel := fixtures.NewAdvancingClock(context.Background())
	defer cancel()

	cli.SendMetricsAsync(ctx, twoCounters(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.NoError(t, err)
	}
	assert.EqualValues(t, 2, requestNum)
	assert.EqualValues(t, cap(cli.reqBufferSem), len(cli.reqBufferSem))
}

func TestSendMetricsInMultipleBatches(t *testing.T) {
	t.Parallel()
	var requestNum uint32
	mux := http.NewServeMux()
	mux.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
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
	v.Set("transport.default.client-timeout", 1*time.Second)
	p := transport.NewTransportPool(logrus.New(), v)
	client, err := NewClient(
		ts.URL,
		false,
		"creds",
		defaultMaxRequests,
		defaultMaxRequestElapsedTime,
		1,
		"default",
		configV1{
			database:        "database",
			retentionPolicy: "rp",
			consistency:     "consistency",
		},
		gostatsd.TimerSubtypes{},
		logrus.New(),
		p,
	)
	require.NoError(t, err)

	res := make(chan []error, 1)
	ctx, cancel := fixtures.NewAdvancingClock(context.Background())
	defer cancel()

	client.SendMetricsAsync(ctx, twoCounters(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.NoError(t, err)
	}
	assert.EqualValues(t, 2, requestNum)
	assert.EqualValues(t, cap(client.reqBufferSem), len(client.reqBufferSem))
}

func TestSendMetrics(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return
		}
		enc := r.Header.Get("Content-Encoding")
		if enc == "gzip" {
			decompressor, err := gzip.NewReader(bytes.NewReader(data))
			if !assert.NoError(t, err) {
				return
			}
			data, err = ioutil.ReadAll(decompressor)
			assert.NoError(t, err)
		}
		expected := strings.Join([]string{
			"c1,unnamed=tag1 count=5,rate=1.1 1",
			"t1,unnamed=tag2 lower=0,upper=1,count=1,rate=1.1,mean=0.5,median=0.5,stddev=0.1,sum=1,sum_squares=1,count_90=0.1 1",
			"g1,unnamed=tag3 value=3 1",
			"users,unnamed=tag4 count=3 1",
			"", // there's always a trailing new line, even on the very last metric
		}, "\n")

		assert.Equal(t, expected, string(data))
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	v := viper.New()
	v.Set("transport.default.client-timeout", 1*time.Second)
	p := transport.NewTransportPool(logrus.New(), v)
	cli, err := NewClient(
		ts.URL,
		true,
		"creds",
		defaultMaxRequests,
		defaultMaxRequestElapsedTime,
		defaultMetricsPerBatch,
		"default",
		configV1{
			database:        "database",
			retentionPolicy: "rp",
			consistency:     "consistency",
		},
		gostatsd.TimerSubtypes{},
		logrus.New(),
		p,
	)
	require.NoError(t, err)

	res := make(chan []error, 1)
	ctx, cancel := fixtures.NewAdvancingClock(context.Background())
	defer cancel()

	cli.SendMetricsAsync(ctx, metricsOneOfEach(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.NoError(t, err)
	}
	assert.EqualValues(t, cap(cli.reqBufferSem), len(cli.reqBufferSem))
}

// twoCounters returns two counters.
func twoCounters() *gostatsd.MetricMap {
	return &gostatsd.MetricMap{
		Counters: gostatsd.Counters{
			"stat1": map[string]gostatsd.Counter{
				"tag1": gostatsd.NewCounter(gostatsd.Nanotime(time.Now().UnixNano()), 5, "", nil),
			},
			"stat2": map[string]gostatsd.Counter{
				"tag2": gostatsd.NewCounter(gostatsd.Nanotime(time.Now().UnixNano()), 50, "", nil),
			},
		},
	}
}

// nolint:dupl
func metricsOneOfEach() *gostatsd.MetricMap {
	return &gostatsd.MetricMap{
		Counters: gostatsd.Counters{
			"c1": map[string]gostatsd.Counter{
				"tag1": {PerSecond: 1.1, Value: 5, Timestamp: gostatsd.Nanotime(100), Hostname: "h1", Tags: gostatsd.Tags{"tag1"}},
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
					Timestamp: gostatsd.Nanotime(200),
					Hostname:  "h2",
					Tags:      gostatsd.Tags{"tag2"},
				},
			},
		},
		Gauges: gostatsd.Gauges{
			"g1": map[string]gostatsd.Gauge{
				"tag3": {Value: 3, Timestamp: gostatsd.Nanotime(300), Hostname: "h3", Tags: gostatsd.Tags{"tag3"}},
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
					Timestamp: gostatsd.Nanotime(400),
					Hostname:  "h4",
					Tags:      gostatsd.Tags{"tag4"},
				},
			},
		},
	}
}

func TestSendHistogram(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return
		}
		enc := r.Header.Get("Content-Encoding")
		if enc == "gzip" {
			decompressor, err := gzip.NewReader(bytes.NewReader(data))
			if !assert.NoError(t, err) {
				return
			}
			data, err = ioutil.ReadAll(decompressor)
			assert.NoError(t, err)
		}
		strData := string(data)

		if !strings.HasPrefix(strData, "t1,gsd_histogram=20_30_40_50_60,unnamed=tag2 ") {
			fmt.Printf("%s\n", strData)
			assert.Fail(t, "expecting prefix `t1,gsd_histogram=20_30_40_50_60,unnamed=tag2 `")
		}
		// not ideal because it doesn't check for the entire field, but we're doing a test, not writing an influxdb parser.
		expected := []string{`le.20=5`, `le.30=10`, `le.40=10`, `le.50=10`, `le.60=19`, `le.+Inf=19`}
		for _, e := range expected {
			assert.Contains(t, strData, e)
		}
		if !strings.HasSuffix(strData, " 1\n") {
			fmt.Printf("%s\n", strData)
			assert.Fail(t, "expecting suffix ` 1`")
		}
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	v := viper.New()
	v.Set("transport.default.client-timeout", 1*time.Second)
	p := transport.NewTransportPool(logrus.New(), v)
	cli, err := NewClient(
		ts.URL,
		true,
		"creds",
		defaultMaxRequests,
		defaultMaxRequestElapsedTime,
		defaultMetricsPerBatch,
		"default",
		configV1{
			database:        "database",
			retentionPolicy: "rp",
			consistency:     "consistency",
		},
		gostatsd.TimerSubtypes{},
		logrus.New(),
		p,
	)
	require.NoError(t, err)

	res := make(chan []error, 1)
	ctx, cancel := fixtures.NewAdvancingClock(context.Background())
	defer cancel()

	cli.SendMetricsAsync(ctx, metricsWithHistogram(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.NoError(t, err)
	}
	assert.EqualValues(t, cap(cli.reqBufferSem), len(cli.reqBufferSem))
}

func metricsWithHistogram() *gostatsd.MetricMap {
	return &gostatsd.MetricMap{
		Timers: gostatsd.Timers{
			"t1": map[string]gostatsd.Timer{
				"tag2": {
					Values:    []float64{0, 1},
					Timestamp: gostatsd.Nanotime(200),
					Hostname:  "h2",
					Tags:      gostatsd.Tags{"tag2", "gsd_histogram:20_30_40_50_60"},
					Histogram: map[gostatsd.HistogramThreshold]int{
						20:                                       5,
						30:                                       10,
						40:                                       10,
						50:                                       10,
						60:                                       19,
						gostatsd.HistogramThreshold(math.Inf(1)): 19,
					},
				},
			},
		},
	}
}

func TestSendEvent(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()

	expectedEvents := []string{
		"events,alerttype=info,host=test-host,priority=normal,sourcetypename=test-sourcename title=\"test-title\",text=\"test-text\" 100\n",
		"events,alerttype=info,host=tag-host,priority=normal,sourcetypename=test-sourcename title=\"test-title\",text=\"test-text\" 100\n",
		"events,alerttype=info,host=test-host,priority=normal,sourcetypename=test-sourcename title=\"test-title\",text=\"test-text\",tags=\"abc,def\" 100\n",
	}

	mux.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return
		}
		enc := r.Header.Get("Content-Encoding")
		if enc == "gzip" {
			decompressor, err := gzip.NewReader(bytes.NewReader(data))
			if !assert.NoError(t, err) {
				return
			}
			data, err = ioutil.ReadAll(decompressor)
			assert.NoError(t, err)
		}

		assert.Contains(t, expectedEvents, string(data))

		// filter out the event if we found it
		next := 0
		for _, expected := range expectedEvents {
			if string(data) != expected {
				expectedEvents[next] = expected
				next++
			}
		}
		expectedEvents = expectedEvents[:next]
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	v := viper.New()
	v.Set("transport.default.client-timeout", 1*time.Second)
	p := transport.NewTransportPool(logrus.New(), v)
	cli, err := NewClient(
		ts.URL,
		true,
		"creds",
		defaultMaxRequests,
		defaultMaxRequestElapsedTime,
		defaultMetricsPerBatch,
		"default",
		configV1{
			database:        "database",
			retentionPolicy: "rp",
			consistency:     "consistency",
		},
		gostatsd.TimerSubtypes{},
		logrus.New(),
		p,
	)
	require.NoError(t, err)

	ctx, cancel := fixtures.NewAdvancingClock(context.Background())
	defer cancel()

	err = cli.SendEvent(ctx, &gostatsd.Event{
		Title:          "test-title",
		Text:           "test-text",
		DateHappened:   time.Unix(100, 0).Unix(),
		Hostname:       "test-host",
		AggregationKey: "test-ak",
		SourceTypeName: "test-sourcename",
		Tags:           nil,
		SourceIP:       "test-sourceip",
		Priority:       0,
		AlertType:      0,
	})
	require.NoError(t, err)

	err = cli.SendEvent(ctx, &gostatsd.Event{
		Title:          "test-title",
		Text:           "test-text",
		DateHappened:   time.Unix(100, 0).Unix(),
		Hostname:       "test-host",
		AggregationKey: "test-ak",
		SourceTypeName: "test-sourcename",
		Tags: gostatsd.Tags{
			"host:tag-host",
		},
		SourceIP:  "test-sourceip",
		Priority:  0,
		AlertType: 0,
	})
	require.NoError(t, err)

	err = cli.SendEvent(ctx, &gostatsd.Event{
		Title:          "test-title",
		Text:           "test-text",
		DateHappened:   time.Unix(100, 0).Unix(),
		Hostname:       "test-host",
		AggregationKey: "test-ak",
		SourceTypeName: "test-sourcename",
		Tags: gostatsd.Tags{
			"eventtags:abc",
			"eventtags:def",
		},
		SourceIP:  "test-sourceip",
		Priority:  0,
		AlertType: 0,
	})
	require.NoError(t, err)

	require.Len(t, expectedEvents, 0, "unmatched events")
	require.EqualValues(t, cap(cli.reqBufferSem), len(cli.reqBufferSem))
}
