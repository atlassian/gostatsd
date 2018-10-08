package newrelic

import (
	"strconv"
	"strings"

	"github.com/atlassian/gostatsd"
)

// flush represents a send operation.
type flush struct {
	ts               *timeSeries
	timestamp        float64
	flushIntervalSec float64
	metricsPerBatch  uint
	cb               func(*timeSeries)
}

// timeSeries represents a time series data structure.
type timeSeries struct {
	Metrics []interface{} `json:"metrics"`
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

// Metric represents a metric data structure for New Relic
type Metric struct {
	Interval    float64      `json:"interval,omitempty"`
	EventType   string       `json:"event_type"`
	Timestamp   float64      `json:"timestamp"`
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

// addMetric adds a metric to the series.
func (f *flush) addMetric(n *Client, metricType string, value float64, persecond float64, hostname string, tags gostatsd.Tags, name string, timestamp gostatsd.Nanotime) {
	standardMetric := setDefaultMetricSet(n, f, name, metricType, value, tags, timestamp)
	if metricType == "counter" {
		standardMetric[n.metricPerSecond] = persecond
	}

	f.ts.Metrics = append(f.ts.Metrics, standardMetric)
}

// addMetric adds a timer metric to the series.
func (f *flush) addTimerMetric(n *Client, metricType string, timer gostatsd.Timer, tagsKey, name string) {
	timerMetric := setDefaultMetricSet(n, f, name, metricType, float64(timer.Count), timer.Tags, timer.Timestamp)

	if !n.disabledSubtypes.Lower {
		timerMetric[n.timerMin] = timer.Min
	}
	if !n.disabledSubtypes.Upper {
		timerMetric[n.timerMax] = timer.Max
	}
	if !n.disabledSubtypes.Count {
		timerMetric[n.timerCount] = float64(timer.Count)
	}
	if !n.disabledSubtypes.CountPerSecond {
		timerMetric[n.metricPerSecond] = timer.PerSecond
	}
	if !n.disabledSubtypes.Mean {
		timerMetric[n.timerMean] = timer.Mean
	}
	if !n.disabledSubtypes.Median {
		timerMetric[n.timerMedian] = timer.Median
	}
	if !n.disabledSubtypes.StdDev {
		timerMetric[n.timerStdDev] = timer.StdDev
	}
	if !n.disabledSubtypes.Sum {
		timerMetric[n.timerSum] = timer.Sum
	}
	if !n.disabledSubtypes.SumSquares {
		timerMetric[n.timerSumSquares] = timer.SumSquares
	}
	for _, pct := range timer.Percentiles {
		timerMetric[pct.Str] = pct.Float
	}
	f.ts.Metrics = append(f.ts.Metrics, timerMetric)
}

func (f *flush) maybeFlush() {
	if uint(len(f.ts.Metrics))+20 >= f.metricsPerBatch { // flush before it reaches max size and grows the slice
		f.cb(f.ts)
		f.ts = &timeSeries{
			Metrics: make([]interface{}, 0, f.metricsPerBatch),
		}
	}
}

func (f *flush) finish() {
	if len(f.ts.Metrics) > 0 {
		f.cb(f.ts)
	}
}

func setDefaultMetricSet(n *Client, f *flush, metricName, Type string, Value float64, tags gostatsd.Tags, timestamp gostatsd.Nanotime) map[string]interface{} {
	defaultMetricSet := map[string]interface{}{}
	defaultMetricSet["interval"] = f.flushIntervalSec
	defaultMetricSet["timestamp"] = timestamp
	defaultMetricSet[n.metricType] = Type
	defaultMetricSet[n.metricName] = metricName
	defaultMetricSet[n.metricValue] = Value
	defaultMetricSet["event_type"] = n.eventType

	defaultMetricSet["integration_version"] = integrationVersion
	defaultMetricSet["protocol_version"] = protocolVersion

	if len(tags) > 0 {
		for _, tag := range tags {
			if tag != "" && strings.Contains(tag, ":") {
				keyvalpair := strings.Split(tag, ":")
				parsed, err := strconv.ParseFloat(keyvalpair[1], 64)
				if err != nil || strings.EqualFold(keyvalpair[1], "infinity") {
					defaultMetricSet[n.tagPrefix+keyvalpair[0]] = keyvalpair[1]
				} else {
					defaultMetricSet[n.tagPrefix+keyvalpair[0]] = parsed
				}
			}
		}
	}

	return defaultMetricSet
}
