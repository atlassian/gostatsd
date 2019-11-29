package cloudwatch

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/transport"
	"github.com/atlassian/gostatsd/pkg/util"

	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	"github.com/spf13/viper"
)

// Maximum number of dimensions per metric
// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html
const MAX_DIMENSIONS = 10

// BackendName is the name of this backend.
const BackendName = "cloudwatch"

type QueueItem struct {
	metrics  []*cloudwatch.MetricDatum
	callback *gostatsd.SendCallback
}

// Client is an object that is used to send messages to AWS CloudWatch.
type Client struct {
	cloudwatch cloudwatchiface.CloudWatchAPI
	namespace  string

	batchDuration time.Duration

	queue      []*QueueItem
	queueMutex sync.RWMutex

	timer      *time.Timer
	timerMutex sync.Mutex

	disabledSubtypes gostatsd.TimerSubtypes
}

// NewClientFromViper constructs a Cloudwatch backend.
func NewClientFromViper(v *viper.Viper, pool *transport.TransportPool) (gostatsd.Backend, error) {
	g := util.GetSubViper(v, "cloudwatch")
	g.SetDefault("namespace", "StatsD")
	g.SetDefault("batchDuration", "50ms")
	g.SetDefault("transport", "default")
	return NewClient(
		g.GetString("namespace"),
		g.GetString("transport"),
		g.GetDuration("batchDuration"),
		gostatsd.DisabledSubMetrics(v),
		pool,
	)
}

// NewClient constructs a AWS Cloudwatch backend.
func NewClient(namespace, transport string, batchDuration time.Duration, disabled gostatsd.TimerSubtypes, pool *transport.TransportPool) (*Client, error) {
	httpClient, err := pool.Get(transport)
	if err != nil {
		return nil, err
	}
	sess, err := session.NewSession(&aws.Config{
		HTTPClient: httpClient.Client,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		cloudwatch:    cloudwatch.New(sess),
		batchDuration: batchDuration,

		timer: nil,

		namespace:        namespace,
		disabledSubtypes: disabled,
	}, nil
}

func extractDimensions(tags gostatsd.Tags) (dimensions []*cloudwatch.Dimension) {
	dimensions = []*cloudwatch.Dimension{}

	for _, tag := range tags {
		key := tag
		value := "set"

		if strings.Contains(tag, ":") {
			segments := strings.SplitN(tag, ":", 2)
			key = segments[0]
			value = segments[1]
		}

		dimensions = append(dimensions, &cloudwatch.Dimension{
			Name:  &key,
			Value: &value,
		})
	}

	// Check that there are not too many dimensions
	dimensionCount := len(dimensions)
	if dimensionCount > MAX_DIMENSIONS {
		log.Warnf("[%s] Too many dimensions (%d) specified, truncating to %d", BackendName, dimensionCount, MAX_DIMENSIONS)
		return dimensions[:MAX_DIMENSIONS]
	}

	return dimensions
}

func (client *Client) buildMetricData(metrics *gostatsd.MetricMap) (metricData []*cloudwatch.MetricDatum) {
	disabled := client.disabledSubtypes

	metricData = []*cloudwatch.MetricDatum{}
	now := time.Now()
	prefix := ""

	addMetricData := func(key string, unit string, value float64, tags gostatsd.Tags) {
		dimensions := extractDimensions(tags)
		key = prefix + key

		metricData = append(metricData, &cloudwatch.MetricDatum{
			MetricName: &key,
			Timestamp:  &now,
			Unit:       &unit,
			Value:      &value,
			Dimensions: dimensions,
		})
	}

	prefix = "stats.counter."
	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		addMetricData(key+".count", "Count", float64(counter.Value), counter.Tags)
		addMetricData(key+".per_second", "Count/Second", counter.PerSecond, counter.Tags)
	})

	prefix = "stats.timers."
	metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		if !disabled.Lower {
			addMetricData(key+".lower", "Milliseconds", timer.Min, timer.Tags)
		}
		if !disabled.Upper {
			addMetricData(key+".upper", "Milliseconds", timer.Max, timer.Tags)
		}
		if !disabled.Count {
			addMetricData(key+".count", "Count", float64(timer.Count), timer.Tags)
		}
		if !disabled.CountPerSecond {
			addMetricData(key+".count_ps", "Count/Second", timer.PerSecond, timer.Tags)
		}
		if !disabled.Mean {
			addMetricData(key+".mean", "Milliseconds", timer.Mean, timer.Tags)
		}
		if !disabled.Median {
			addMetricData(key+".median", "Milliseconds", timer.Median, timer.Tags)
		}
		if !disabled.StdDev {
			addMetricData(key+".std", "Milliseconds", timer.StdDev, timer.Tags)
		}
		if !disabled.Sum {
			addMetricData(key+".sum", "Milliseconds", timer.Sum, timer.Tags)
		}
		if !disabled.SumSquares {
			addMetricData(key+".sum_squares", "Milliseconds", timer.SumSquares, timer.Tags)
		}
		for _, pct := range timer.Percentiles {
			addMetricData(key+"."+pct.Str, "Milliseconds", pct.Float, timer.Tags)
		}
	})

	prefix = "stats.gauge."
	metrics.Gauges.Each(func(key, tagsKey string, gauge gostatsd.Gauge) {
		addMetricData(key, "None", gauge.Value, gauge.Tags)
	})

	prefix = "stats.set."
	metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		addMetricData(key, "None", float64(len(set.Values)), set.Tags)
	})

	return metricData
}

func (client *Client) flush() {
	client.timerMutex.Lock()

	// If there already is a timer, we already have a function waiting to execute
	if client.timer != nil {
		client.timer.Reset(client.batchDuration)
		client.timerMutex.Unlock()
		return
	}

	client.timer = time.NewTimer(client.batchDuration)
	client.timerMutex.Unlock()

	timer := client.timer

	// Wait until timer expires
	<-timer.C

	client.timerMutex.Lock()
	client.timer = nil // Stop current timer
	client.timerMutex.Unlock()

	// Copy pending metric items and create new queue instance
	// client.queue is protected by a Read-Write Lock such
	// that execution is only blocked while replacing the queue
	client.queueMutex.Lock()
	queue := client.queue
	client.queue = []*QueueItem{}
	client.queueMutex.Unlock()

	// Run actual function
	metricData := []*cloudwatch.MetricDatum{}
	for _, item := range queue {
		metricData = append(metricData, item.metrics...)
	}

	start := 0
	requests := 0
	errors := []error{}
	length := len(metricData)
	api := client.cloudwatch

	// Send metrics in batches of 20
	// We are not allowed to add more to a single PutMetricData request
	// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html
	for start < length {
		end := start + 20

		if end > length {
			end = length
		}

		if start >= end {
			// No more metrics to sent
			break
		}

		requests += 1

		data := metricData[start:end]
		start = end

		_, err := api.PutMetricData(&cloudwatch.PutMetricDataInput{
			MetricData: data,
			Namespace:  &client.namespace,
		})

		errors = append(errors, err)
	}

	log.Infof("[%s] Pushed %d metrics in %d requests", BackendName, length, requests)

	for _, q := range queue {
		go (*q.callback)(errors)
	}
}

// SendMetricsAsync sends the metrics in a MetricsMap to AWS Cloudwatch,
// preparing payload synchronously but doing the send asynchronously.
func (client *Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	metricData := client.buildMetricData(metrics)
	length := len(metricData)
	errors := []error{}

	if length < 1 {
		cb(errors)
		return
	}

	// Protect queue
	// We don't want to be inserting into an old queue instance
	// The queue instance is replaced by Client.flush()
	client.queueMutex.RLock()
	client.queue = append(client.queue, &QueueItem{
		metrics:  metricData,
		callback: &cb,
	})
	client.queueMutex.RUnlock()

	go client.flush()
}

// Events currently not supported.
func (client *Client) SendEvent(ctx context.Context, e *gostatsd.Event) (retErr error) {
	return nil
}

// Name returns the name of the backend.
func (*Client) Name() string {
	return BackendName
}
