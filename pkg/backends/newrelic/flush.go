package newrelic

import (
	"fmt"
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

// addMetric adds a metric to the series.
func (f *flush) addMetric(n *Client, metricType string, value float64, persecond float64, hostname string, tags gostatsd.Tags, name string, timestamp gostatsd.Nanotime) {
	if n.flushType == flushTypeMetrics {
		metricName := name
		if metricType == "counter" {
			perSecondMetric := newMetricSet(n, f, metricName+".per_second", "gauge", persecond, tags, timestamp)
			f.ts.Metrics = append(f.ts.Metrics, perSecondMetric)
		}
		standardMetric := newMetricSet(n, f, name, metricType, value, tags, timestamp)
		f.ts.Metrics = append(f.ts.Metrics, standardMetric)
	} else {
		standardMetric := newMetricSet(n, f, name, metricType, value, tags, timestamp)
		if metricType == "counter" {
			standardMetric[n.metricPerSecond] = persecond
		}
		f.ts.Metrics = append(f.ts.Metrics, standardMetric)
	}
}

// addMetric adds a timer metric to the series.
func (f *flush) addTimerMetric(n *Client, metricType string, timer gostatsd.Timer, tagsKey, name string) {
	timerMetric := newMetricSet(n, f, name, metricType, float64(timer.Count), timer.Tags, timer.Timestamp)

	if n.flushType == "metrics" {
		timerMetric["value"] = map[string]float64{
			"count": float64(timer.Count),
			"sum":   timer.Sum,
			"min":   timer.Min,
			"max":   timer.Max,
		}

		if !n.disabledSubtypes.CountPerSecond {
			gaugeMetric := newMetricSet(n, f, name+".per_second", "gauge", timer.PerSecond, timer.Tags, timer.Timestamp)
			f.ts.Metrics = append(f.ts.Metrics, gaugeMetric)
		}
		if !n.disabledSubtypes.Mean {
			gaugeMetric := newMetricSet(n, f, name+".mean", "gauge", timer.Mean, timer.Tags, timer.Timestamp)
			f.ts.Metrics = append(f.ts.Metrics, gaugeMetric)
		}
		if !n.disabledSubtypes.Median {
			gaugeMetric := newMetricSet(n, f, name+".median", "gauge", timer.Median, timer.Tags, timer.Timestamp)
			f.ts.Metrics = append(f.ts.Metrics, gaugeMetric)
		}
		if !n.disabledSubtypes.StdDev {
			gaugeMetric := newMetricSet(n, f, name+".std_dev", "gauge", timer.StdDev, timer.Tags, timer.Timestamp)
			f.ts.Metrics = append(f.ts.Metrics, gaugeMetric)
		}
		if !n.disabledSubtypes.SumSquares {
			gaugeMetric := newMetricSet(n, f, name+".sum_squares", "gauge", timer.SumSquares, timer.Tags, timer.Timestamp)
			f.ts.Metrics = append(f.ts.Metrics, gaugeMetric)
		}

		// https://github.com/newrelic/newrelic-exporter-specs/blob/master/Guidelines.md#percentiles
		// Percentiles MUST be implemented as a set of metrics, one metric for each percentile.
		// Each metric MUST be a Gauge metric.
		// Each metric value MUST represent a calculated percentile.
		// The percentile attribute MUST be included with each metric. This attribute value MUST represent the percentile being measured as a floating-point number within the range [0.0, 100.0].
		// Each metric name MUST have a ".percentiles" suffix.
		for _, pct := range timer.Percentiles {
			lastUnderscore := strings.LastIndex(pct.Str, "_")
			gaugeMetric := newMetricSet(n, f, fmt.Sprintf("%v.%v.percentiles", name, pct.Str[:lastUnderscore]), "gauge", pct.Float, timer.Tags, timer.Timestamp)
			percentileResult, err := strconv.ParseFloat(pct.Str[lastUnderscore+1:], 64) // eg. for sum_squares_90 will return 90
			if err == nil {
				gaugeMetric["attributes"].(map[string]interface{})["percentile"] = percentileResult
				f.ts.Metrics = append(f.ts.Metrics, gaugeMetric)
			}
		}
	} else {
		if !n.disabledSubtypes.Lower {
			timerMetric[n.timerMin] = timer.Min
		}
		if !n.disabledSubtypes.Upper {
			timerMetric[n.timerMax] = timer.Max
		}
		if !n.disabledSubtypes.Count {
			timerMetric[n.timerCount] = float64(timer.Count)
		}
		if !n.disabledSubtypes.Sum {
			timerMetric[n.timerSum] = timer.Sum
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
		if !n.disabledSubtypes.SumSquares {
			timerMetric[n.timerSumSquares] = timer.SumSquares
		}
		for _, pct := range timer.Percentiles {
			timerMetric[pct.Str] = pct.Float
		}
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

func newMetricSet(n *Client, f *flush, metricName, Type string, Value float64, tags gostatsd.Tags, timestamp gostatsd.Nanotime) map[string]interface{} {
	metricSet := map[string]interface{}{}

	// GoStatsD provides the timestamp in Nanotime, New Relic requires seconds or milliseconds, see under "Limits and restricted characters"
	// https://docs.newrelic.com/docs/insights/insights-data-sources/custom-data/send-custom-events-event-api#instrument
	metricSet["timestamp"] = timestamp / 1e9

	if n.flushType == flushTypeMetrics {
		metricSet["name"] = metricName
		if metricSet["attributes"] == nil {
			metricSet["attributes"] = map[string]interface{}{}
		}
		metricSet["attributes"].(map[string]interface{})["statsdType"] = Type
		n.setTags(tags, metricSet["attributes"].(map[string]interface{}))
		switch Type {
		case "timer":
			metricSet["type"] = "summary"
			metricSet["name"] = metricSet["name"].(string) + ".summary"
		case "counter":
			metricSet["type"] = "count"
			metricSet["value"] = Value
		case "gauge":
			metricSet["type"] = Type
			metricSet["value"] = Value
		}
	} else {
		metricSet["interval"] = f.flushIntervalSec
		metricSet["integration_version"] = integrationVersion

		// New Relic Insights Event API, expects the "Event Type" to be in camel case format as opposed to an underscore with the Infrastructure Payload
		// https://docs.newrelic.com/docs/insights/insights-data-sources/custom-data/send-custom-events-event-api#instrument
		// https://github.com/newrelic/infra-integrations-sdk/blob/master/docs/v2tov3.md#v2-json-full-sample

		switch n.flushType {
		case flushTypeInsights:
			metricSet["eventType"] = n.eventType
		default:
			metricSet["event_type"] = n.eventType
		}
		metricSet[n.metricType] = Type
		metricSet[n.metricName] = metricName
		metricSet[n.metricValue] = Value
		n.setTags(tags, metricSet)
	}

	return metricSet
}
