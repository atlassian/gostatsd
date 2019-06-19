package statsd

import (
	"math"
	"strconv"
	"strings"

	"github.com/atlassian/gostatsd"
)

const (
	HistogramThresholdsTagPrefix = "gsd_histogram:"
	HistogramThresholdsSeparator = "_"
)

func LatencyHistogram(timer gostatsd.Timer) map[gostatsd.HistogramThreshold]int {
	result := make(map[gostatsd.HistogramThreshold]int)
	thresholds := retrieveThresholds(timer)

	if thresholds == nil {
		return nil
	}
	infiniteThreshold := gostatsd.HistogramThreshold{Le: math.Inf(1)}

	for _, histogramThreshold := range thresholds {
		result[histogramThreshold] = 0
	}
	result[infiniteThreshold] = 0

	for _, value := range timer.Values {
		for _, latencyBucket := range thresholds {
			if value <= latencyBucket.Le {
				result[latencyBucket] += 1
			}
		}
	}
	result[infiniteThreshold] += len(timer.Values)
	return result
}

func retrieveThresholds(timer gostatsd.Timer) []gostatsd.HistogramThreshold {
	tag := findTag(timer.Tags, HistogramThresholdsTagPrefix)
	if tag != nil {
		bucketsTagValue := (*tag)[len(HistogramThresholdsTagPrefix):]
		stringThresholds := strings.Split(bucketsTagValue, HistogramThresholdsSeparator)
		floatThresholds := mapToThresholds(stringThresholds)
		if floatThresholds == nil {
			return []gostatsd.HistogramThreshold{}
		}
		return floatThresholds
	}
	return nil
}

func mapToThresholds(vs []string) []gostatsd.HistogramThreshold {
	var lb []gostatsd.HistogramThreshold
	for _, v := range vs {
		floatBucket, err := strconv.ParseFloat(v, 64)
		if err == nil {
			lb = append(lb, gostatsd.HistogramThreshold{Le: floatBucket})
		}
	}
	return lb
}

// returns the first string in a that begins with prefix
func findTag(a []string, prefix string) *string {
	for _, n := range a {
		if strings.HasPrefix(n, prefix) {
			return &n
		}
	}
	return nil
}
