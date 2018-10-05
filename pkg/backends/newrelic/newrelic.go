package newrelic

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/cenkalti/backoff"
	"github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Metric struct
type Metric struct {
	EventType   string       `json:"event_type"`
	Timestamp   int64        `json:"timestamp"`
	Name        string       `json:"metric_name"`
	Type        string       `json:"metric_type"`
	Value       float64      `json:"metric_value"`
	PerSecond   float64      `json:"metric_per_second"`
	Count       float64      `json:"metric_count"`
	Min         float64      `json:"samples_min,omitempty"`
	Max         float64      `json:"samples_max,omitempty"`
	Mean        float64      `json:"samples_mean,omitempty"`
	Median      float64      `json:"samples_median,omitempty"`
	StdDev      float64      `json:"samples_std_dev,omitempty"`
	Sum         float64      `json:"samples_sum,omitempty"`
	SumSquares  float64      `json:"samples_sum_squares,omitempty"`
	Percentiles []Percentile `json:",omitempty"`
	Tags        []Tag        `json:",omitempty"`
}

// Tag struct
type Tag struct {
	Key string
	Val string
}

// Percentile struct
type Percentile struct {
	Key string
	Val float64
}

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// BackendName is the name of this backend.
const BackendName = "newrelic"

// Client is an object that is used to send messages to newrelic
type Client struct {
	address   string
	eventType string
	tagPrefix string
	//Options to define your own field names to support other StatsD implementations
	metricName       string
	metricType       string
	metricPerSecond  string
	metricValue      string
	timerValue       string
	timerMin         string
	timerMax         string
	timerMean        string
	timerMedian      string
	timerStdDev      string
	timerSum         string
	timerSumSquares  string
	disabledSubtypes gostatsd.TimerSubtypes
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	return n
}

// NewClientFromViper constructs a newrelic backend
func NewClientFromViper(v *viper.Viper) (gostatsd.Backend, error) {
	nr := getSubViper(v, "newrelic")
	nr.SetDefault("address", "localhost:8001")
	nr.SetDefault("event_type", "StatsD")
	nr.SetDefault("tag_prefix", "")
	nr.SetDefault("metric_name", "metric_name")
	nr.SetDefault("metric_type", "metric_type")
	nr.SetDefault("metric_per_second", "metric_per_second")
	nr.SetDefault("metric_value", "metric_value")
	nr.SetDefault("timer_value", "samples_count")
	nr.SetDefault("timer_min", "samples_min")
	nr.SetDefault("timer_max", "samples_max")
	nr.SetDefault("timer_mean", "samples_mean")
	nr.SetDefault("timer_median", "samples_median")
	nr.SetDefault("timer_std_dev", "samples_std_dev")
	nr.SetDefault("timer_sum", "samples_sum")
	nr.SetDefault("timer_sum_square", "samples_sum_squares")

	return NewClient(
		nr.GetString("address"),
		nr.GetString("event_type"),
		nr.GetString("tag_prefix"),
		nr.GetString("metric_name"),
		nr.GetString("metric_type"),
		nr.GetString("metric_per_second"),
		nr.GetString("metric_value"),
		nr.GetString("timer_value"),
		nr.GetString("timer_min"),
		nr.GetString("timer_max"),
		nr.GetString("timer_mean"),
		nr.GetString("timer_median"),
		nr.GetString("timer_std_dev"),
		nr.GetString("timer_sum"),
		nr.GetString("timer_sum_square"),
		gostatsd.DisabledSubMetrics(v),
	)
}

// NewClient constructs a newrelic backend.
func NewClient(address, eventType, tagPrefix,
	metricName, metricType, metricPerSecond, metricValue,
	timerValue, timerMin, timerMax, timerMean, timerMedian, timerStdDev, timerSum, timerSumSquares string,
	disabled gostatsd.TimerSubtypes) (*Client, error) {

	return &Client{
		address:          address,
		eventType:        eventType,
		tagPrefix:        tagPrefix,
		metricName:       metricName,
		metricType:       metricType,
		metricPerSecond:  metricPerSecond,
		metricValue:      metricValue,
		timerValue:       timerValue,
		timerMin:         timerMin,
		timerMax:         timerMax,
		timerMean:        timerMean,
		timerMedian:      timerMedian,
		timerStdDev:      timerStdDev,
		timerSum:         timerSum,
		timerSumSquares:  timerSumSquares,
		disabledSubtypes: disabled,
	}, nil
}

// SendMetricsAsync prints the metrics in a MetricsMap for newrelic, preparing payload synchronously but doing the send asynchronously.
func (client Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	statsdMetrics := client.preparePayload(metrics, &client.disabledSubtypes)
	go func() {
		cb([]error{client.sendPayload(statsdMetrics, &client.disabledSubtypes)})
	}()
}

func (client Client) sendPayload(metricSets []Metric, disabled *gostatsd.TimerSubtypes) (retErr error) {
	for ms := range metricSets {

		go func(ms int) {
			metrics := map[string]interface{}{}
			metrics["event_type"] = metricSets[ms].EventType
			metrics["timestamp"] = metricSets[ms].Timestamp
			metrics[client.metricName] = metricSets[ms].Name
			metrics[client.metricType] = metricSets[ms].Type
			metrics[client.metricValue] = metricSets[ms].Value

			if metricSets[ms].Type == "counter" || metricSets[ms].Type == "timer" {
				metrics[client.metricPerSecond] = metricSets[ms].PerSecond
			}

			for _, tag := range metricSets[ms].Tags {
				metrics[client.tagPrefix+tag.Key] = tag.Val
			}

			if metricSets[ms].Type == "timer" {
				if !disabled.Count {
					metrics[client.timerValue] = metricSets[ms].Count
				}
				if !disabled.Lower {
					metrics[client.timerMin] = metricSets[ms].Min
				}
				if !disabled.Upper {
					metrics[client.timerMax] = metricSets[ms].Max
				}
				if !disabled.Mean {
					metrics[client.timerMean] = metricSets[ms].Mean
				}
				if !disabled.Median {
					metrics[client.timerMedian] = metricSets[ms].Median
				}
				if !disabled.StdDev {
					metrics[client.timerStdDev] = metricSets[ms].StdDev
				}
				if !disabled.Sum {
					metrics[client.timerSum] = metricSets[ms].Sum
				}
				if !disabled.SumSquares {
					metrics[client.timerSumSquares] = metricSets[ms].SumSquares
				}
				for _, percentile := range metricSets[ms].Percentiles {
					metrics[percentile.Key] = percentile.Val
				}
			}

			v2Payload := map[string]interface{}{
				"name":                "com.nr.gostatsd-server",
				"protocol_version":    "2",
				"integration_version": "2.0.0",
				"data": []interface{}{
					map[string]interface{}{
						"metrics": []interface{}{metrics},
					},
				},
			}

			mJSON, _ := json.Marshal(v2Payload)

			b := backoff.NewExponentialBackOff()
			b.MaxElapsedTime = 15 * time.Second
			ticker := backoff.NewTicker(b)

			for range ticker.C {
				req, _ := http.NewRequest("POST", "http://"+client.address+"/v1/data", bytes.NewBuffer(mJSON))
				req.Header.Set("Content-Type", "application/json")
				hclient := &http.Client{}
				_, err := hclient.Do(req)

				if err != nil {
					log.Debug("Failed to send payload:", err)
					log.Debug("Retrying...")
					continue
				}
				ticker.Stop()
				break
			}

		}(ms)
	}

	return nil
}

// createTags split key:val pairs
func createTags(tagsKey string) (metricTags []Tag) {
	metricTags = Metric{}.Tags
	tags := strings.Split(tagsKey, ",")

	if len(tags) > 0 {
		for _, tag := range tags {
			if tag != "" && strings.Contains(tag, ":") {
				keyvalpair := strings.Split(tag, ":")
				metricTags = append(metricTags, Tag{Key: keyvalpair[0], Val: keyvalpair[1]})
			}
		}
	}

	return metricTags
}

func (client Client) preparePayload(metrics *gostatsd.MetricMap, disabled *gostatsd.TimerSubtypes) []Metric {
	now := time.Now().Unix()
	var metricSets []Metric

	metrics.Gauges.Each(func(key, tagsKey string, gauge gostatsd.Gauge) {
		metricTags := createTags(tagsKey)
		metricSet := createDefaultMetricSet(client, "gauge", now, metricTags, key, gauge.Value)
		metricSets = append(metricSets, metricSet)
	})

	metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		metricTags := createTags(tagsKey)
		metricSet := createDefaultMetricSet(client, "set", now, metricTags, key, float64(len(set.Values)))
		metricSets = append(metricSets, metricSet)
	})

	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		metricTags := createTags(tagsKey)
		metricSet := createDefaultMetricSet(client, "counter", now, metricTags, key, float64(counter.Value))
		metricSet.PerSecond = counter.PerSecond
		metricSets = append(metricSets, metricSet)
	})

	metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		metricTags := createTags(tagsKey)
		metricSet := createDefaultMetricSet(client, "timer", now, metricTags, key, 0)

		if !disabled.Count {
			metricSet.Count = float64(timer.Count)
		}
		if !disabled.Lower {
			metricSet.Min = timer.Min
		}
		if !disabled.Upper {
			metricSet.Max = timer.Max
		}
		if !disabled.CountPerSecond {
			metricSet.PerSecond = timer.PerSecond
		}
		if !disabled.Mean {
			metricSet.Mean = timer.Mean
		}
		if !disabled.Median {
			metricSet.Median = timer.Median
		}
		if !disabled.StdDev {
			metricSet.StdDev = timer.StdDev
		}
		if !disabled.Sum {
			metricSet.Sum = timer.Sum
		}
		if !disabled.SumSquares {
			metricSet.SumSquares = timer.SumSquares
		}

		for _, pct := range timer.Percentiles {
			metricSet.Percentiles = append(metricSet.Percentiles, Percentile{Key: pct.Str, Val: pct.Float})
		}

		metricSets = append(metricSets, metricSet)
	})

	return metricSets
}

func createDefaultMetricSet(client Client, Type string, Now int64, metricTags []Tag, metricName string, Value float64) Metric {
	defaultMetricSet := Metric{}
	defaultMetricSet.Type = Type
	defaultMetricSet.Timestamp = Now
	defaultMetricSet.Tags = metricTags
	defaultMetricSet.Name = metricName
	defaultMetricSet.Value = Value
	defaultMetricSet.EventType = client.eventType
	return defaultMetricSet
}

// SendEvent prints events
func (client Client) SendEvent(ctx context.Context, e *gostatsd.Event) (retErr error) {
	return nil
}

// Name returns the name of the backend.
func (Client) Name() string {
	return BackendName
}
