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

// addMetric adds a metric to the series.
func (f *flush) addMetric(n *Client, metricType string, value float64, persecond float64, hostname string, tags gostatsd.Tags, name string, timestamp gostatsd.Nanotime) {
	standardMetric := newMetricSet(n, f, name, metricType, value, tags, timestamp)
	if metricType == "counter" {
		standardMetric[n.metricPerSecond] = persecond
	}
	f.ts.Metrics = append(f.ts.Metrics, standardMetric)
}

// addMetric adds a timer metric to the series.
func (f *flush) addTimerMetric(n *Client, metricType string, timer gostatsd.Timer, tagsKey, name string) {
	timerMetric := newMetricSet(n, f, name, metricType, float64(timer.Count), timer.Tags, timer.Timestamp)

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

func newMetricSet(n *Client, f *flush, metricName, Type string, Value float64, tags gostatsd.Tags, timestamp gostatsd.Nanotime) map[string]interface{} {
	metricSet := map[string]interface{}{}
	metricSet["interval"] = f.flushIntervalSec
	metricSet["timestamp"] = timestamp
	metricSet["event_type"] = n.eventType
	metricSet["integration_version"] = integrationVersion

	metricSet[n.metricType] = Type
	metricSet[n.metricName] = metricName
	metricSet[n.metricValue] = Value

	for _, tag := range tags {
		if strings.Contains(tag, ":") {
			keyvalpair := strings.SplitN(tag, ":", 2)
			parsed, err := strconv.ParseFloat(keyvalpair[1], 64)
			if err != nil || strings.EqualFold(keyvalpair[1], "infinity") {
				metricSet[n.tagPrefix+keyvalpair[0]] = keyvalpair[1]
			} else {
				metricSet[n.tagPrefix+keyvalpair[0]] = parsed
			}
		} else {
			metricSet[n.tagPrefix+tag] = "true"
		}
	}

	return metricSet
}
