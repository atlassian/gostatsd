package cloudwatch

import (
	"context"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/util"
	"github.com/atlassian/gostatsd/pkg/transport"
)

// Maximum number of dimensions per metric
// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html
const MAX_DIMENSIONS = 10

// BackendName is the name of this backend.
const BackendName = "cloudwatch"

type CloudwatchClient interface {
	PutMetricData(context.Context, *cloudwatch.PutMetricDataInput, ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricDataOutput, error)
}

// Client is an object that is used to send messages to AWS CloudWatch.
type Client struct {
	logger logrus.FieldLogger

	cloudwatch CloudwatchClient
	namespace  string

	disabledSubtypes gostatsd.TimerSubtypes
}

// NewClientFromViper constructs a Cloudwatch backend.
func NewClientFromViper(v *viper.Viper, logger logrus.FieldLogger, pool *transport.TransportPool) (gostatsd.Backend, error) {
	g := util.GetSubViper(v, "cloudwatch")
	g.SetDefault("namespace", "StatsD")
	g.SetDefault("transport", "default")

	return NewClient(
		g.GetString("namespace"),
		g.GetString("transport"),
		gostatsd.DisabledSubMetrics(v),
		logger,
		pool,
	)
}

// NewClient constructs a AWS Cloudwatch backend.
func NewClient(namespace, transport string, disabled gostatsd.TimerSubtypes, logger logrus.FieldLogger, pool *transport.TransportPool) (*Client, error) {
	httpClient, err := pool.Get(transport)
	if err != nil {
		return nil, err
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithHTTPClient(httpClient.Client))
	if err != nil {
		return nil, err
	}

	return &Client{
		logger: logger,

		cloudwatch: cloudwatch.NewFromConfig(cfg),
		namespace:  namespace,

		disabledSubtypes: disabled,
	}, nil
}

func (client *Client) extractDimensions(tags gostatsd.Tags) (dimensions []types.Dimension) {
	dimensions = []types.Dimension{}

	for _, tag := range tags {
		key := tag
		value := "set"

		if strings.Contains(tag, ":") {
			segments := strings.SplitN(tag, ":", 2)
			key = segments[0]
			value = segments[1]
		}

		dimensions = append(dimensions, types.Dimension{
			Name:  &key,
			Value: &value,
		})
	}

	// Check that there are not too many dimensions
	dimensionCount := len(dimensions)
	if dimensionCount > MAX_DIMENSIONS {
		client.logger.WithFields(logrus.Fields{
			"count": dimensionCount,
			"limit": MAX_DIMENSIONS,
		}).Warn("too many dimensions, truncated")
		return dimensions[:MAX_DIMENSIONS]
	}

	return dimensions
}

func (client *Client) buildMetricData(metrics *gostatsd.MetricMap) (metricData []types.MetricDatum) {
	disabled := client.disabledSubtypes

	metricData = []types.MetricDatum{}
	now := time.Now()
	prefix := ""

	addMetricData := func(key string, unit string, value float64, tags gostatsd.Tags) {
		dimensions := client.extractDimensions(tags)
		key = prefix + key

		metricData = append(metricData, types.MetricDatum{
			MetricName: &key,
			Timestamp:  &now,
			Unit:       types.StandardUnit(unit),
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
		if timer.Histogram != nil {
			for histogramThreshold, count := range timer.Histogram {
				bucketTag := "le:+Inf"
				if !math.IsInf(float64(histogramThreshold), 1) {
					bucketTag = "le:" + strconv.FormatFloat(float64(histogramThreshold), 'f', -1, 64)
				}
				newTags := timer.Tags.Concat(gostatsd.Tags{bucketTag})
				addMetricData(key+".histogram", "Count", float64(count), newTags)
			}
		} else {
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

// SendMetricsAsync sends the metrics in a MetricsMap to AWS Cloudwatch,
// preparing payload synchronously but doing the send asynchronously.
func (client *Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	api := client.cloudwatch
	metricData := client.buildMetricData(metrics)
	length := len(metricData)
	errors := []error{}

	if length < 1 {
		cb(errors)
		return
	}

	go func() {
		start := 0

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

			data := metricData[start:end]
			start = end

			_, err := api.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
				MetricData: data,
				Namespace:  &client.namespace,
			})

			errors = append(errors, err)
		}

		cb(errors)
	}()
}

// Events currently not supported.
func (client *Client) SendEvent(ctx context.Context, e *gostatsd.Event) (retErr error) {
	return nil
}

// Name returns the name of the backend.
func (Client) Name() string {
	return BackendName
}
