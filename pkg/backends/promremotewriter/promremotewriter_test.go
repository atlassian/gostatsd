package promremotewriter

import (
	"context"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tilinna/clock"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pb"
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
	mux.HandleFunc("/api/push", func(w http.ResponseWriter, r *http.Request) {
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
	client, err := NewClient(ts.URL+"/api/push", "agent", "default", defaultMetricsPerBatch, defaultMaxRequests, 2*time.Second, 1*time.Second, gostatsd.TimerSubtypes{}, logrus.New(), p)
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

func TestSendMetricsInMultipleBatches(t *testing.T) {
	t.Parallel()
	var requestNum uint32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/push", func(w http.ResponseWriter, r *http.Request) {
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
	client, err := NewClient(ts.URL+"/api/push", "agent", "default", 1, defaultMaxRequests, 2*time.Second, 1*time.Second, gostatsd.TimerSubtypes{}, logrus.New(), p)
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
	mux := http.NewServeMux()
	mux.HandleFunc("/api/push", func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return
		}
		require.Equal(t, "snappy", r.Header.Get("Content-Encoding"))
		decoded, err := snappy.Decode(nil, data)
		require.NoError(t, err)
		var writeReq pb.PromWriteRequest
		err = proto.Unmarshal(decoded, &writeReq)
		require.NoError(t, err)
		expected := &pb.PromWriteRequest{
			Timeseries: []*pb.PromTimeSeries{
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "c1"}, {Name: "unnamed", Value: "tag1"}}, Samples: []*pb.PromSample{{Value: 1.1, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "c1_count"}, {Name: "unnamed", Value: "tag1"}}, Samples: []*pb.PromSample{{Value: 5, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_lower"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 0, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_upper"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 1, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_count"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 1, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_count_ps"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 1.1, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_mean"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 0.5, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_median"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 0.5, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_std"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 0.1, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_sum"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 1, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_sum_squares"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 1, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_count_90"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 0.1, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "g1"}, {Name: "unnamed", Value: "tag3"}}, Samples: []*pb.PromSample{{Value: 3, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "users"}, {Name: "unnamed", Value: "tag4"}}, Samples: []*pb.PromSample{{Value: 3, Timestamp: 100000}}},
			},
		}
		assert.Equal(t, expected, &writeReq)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	v := viper.New()
	v.Set("transport.default.client-timeout", 1*time.Second)
	p := transport.NewTransportPool(logrus.New(), v)
	cli, err := NewClient(ts.URL+"/api/push", "agent", "default", 1000, defaultMaxRequests, 2*time.Second, 1100*time.Millisecond, gostatsd.TimerSubtypes{}, logrus.New(), p)
	require.NoError(t, err)

	c := clock.NewMock(time.Unix(100, 0))
	ctx := clock.Context(context.Background(), c)
	res := make(chan []error, 1)
	cli.SendMetricsAsync(ctx, metricsOneOfEach(), func(errs []error) {
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
				"tag1": {PerSecond: 1.1, Value: 5, Timestamp: gostatsd.Nanotime(100), Source: "h1", Tags: gostatsd.Tags{"tag1"}},
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
					Source:    "h2",
					Tags:      gostatsd.Tags{"tag2"},
				},
			},
		},
		Gauges: gostatsd.Gauges{
			"g1": map[string]gostatsd.Gauge{
				"tag3": {Value: 3, Timestamp: gostatsd.Nanotime(300), Source: "h3", Tags: gostatsd.Tags{"tag3"}},
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
					Source:    "h4",
					Tags:      gostatsd.Tags{"tag4"},
				},
			},
		},
	}
}

func TestSendHistogram(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/push", func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return
		}
		require.Equal(t, "snappy", r.Header.Get("Content-Encoding"))
		decoded, err := snappy.Decode(nil, data)
		require.NoError(t, err)
		var writeReq pb.PromWriteRequest
		err = proto.Unmarshal(decoded, &writeReq)
		require.NoError(t, err)
		expected := &pb.PromWriteRequest{
			Timeseries: []*pb.PromTimeSeries{
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_histogram"}, {Name: "gsd_histogram", Value: "20_30_40_50_60"}, {Name: "le", Value: "+Inf"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 19, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_histogram"}, {Name: "gsd_histogram", Value: "20_30_40_50_60"}, {Name: "le", Value: "20"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 5, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_histogram"}, {Name: "gsd_histogram", Value: "20_30_40_50_60"}, {Name: "le", Value: "30"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 10, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_histogram"}, {Name: "gsd_histogram", Value: "20_30_40_50_60"}, {Name: "le", Value: "40"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 10, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_histogram"}, {Name: "gsd_histogram", Value: "20_30_40_50_60"}, {Name: "le", Value: "50"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 10, Timestamp: 100000}}},
				{Labels: []*pb.PromLabel{{Name: "__name__", Value: "t1_histogram"}, {Name: "gsd_histogram", Value: "20_30_40_50_60"}, {Name: "le", Value: "60"}, {Name: "unnamed", Value: "tag2"}}, Samples: []*pb.PromSample{{Value: 19, Timestamp: 100000}}},
			},
		}
		sort.Slice(writeReq.Timeseries, func(i, j int) bool {
			ts1 := writeReq.Timeseries[i]
			ts2 := writeReq.Timeseries[j]
			return ts1.Labels[2].Value < ts2.Labels[2].Value
		})
		assert.Equal(t, expected, &writeReq)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	v := viper.New()
	v.Set("transport.default.client-timeout", 1*time.Second)
	p := transport.NewTransportPool(logrus.New(), v)
	client, err := NewClient(ts.URL+"/api/push", "agent", "default", 1000, defaultMaxRequests, 2*time.Second, 1100*time.Millisecond, gostatsd.TimerSubtypes{}, logrus.New(), p)
	require.NoError(t, err)
	ctx := clock.Context(context.Background(), clock.NewMock(time.Unix(100, 0)))
	res := make(chan []error, 1)
	client.SendMetricsAsync(ctx, metricsWithHistogram(), func(errs []error) {
		res <- errs
	})
	errs := <-res
	for _, err := range errs {
		assert.NoError(t, err)
	}
}

func metricsWithHistogram() *gostatsd.MetricMap {
	return &gostatsd.MetricMap{
		Timers: gostatsd.Timers{
			"t1": map[string]gostatsd.Timer{
				"tag2": {
					Values:    []float64{0, 1},
					Timestamp: gostatsd.Nanotime(200),
					Source:    "h2",
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
