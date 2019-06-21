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

func latencyHistogram(timer gostatsd.Timer, bucketLimit uint32) map[gostatsd.HistogramThreshold]int {
	result := make(map[gostatsd.HistogramThreshold]int)

	if bucketLimit==0 {
		return result
	}

	thresholds := retrieveThresholds(timer, bucketLimit)

	if thresholds == nil {
		return nil
	}
	infiniteThreshold := gostatsd.HistogramThreshold(math.Inf(1))

	for _, histogramThreshold := range thresholds {
		result[histogramThreshold] = 0
	}
	result[infiniteThreshold] = 0

	for _, value := range timer.Values {
		for _, latencyBucket := range thresholds {
			if value <= float64(latencyBucket) {
				result[latencyBucket] += 1
			}
		}
	}
	result[infiniteThreshold] += len(timer.Values)

	return result
}

func retrieveThresholds(timer gostatsd.Timer, bucketlimit uint32) []gostatsd.HistogramThreshold {
	tag, found := findTag(timer.Tags, HistogramThresholdsTagPrefix)
	if found {
		bucketsTagValue := tag[len(HistogramThresholdsTagPrefix):]
		stringThresholds := strings.Split(bucketsTagValue, HistogramThresholdsSeparator)
		floatThresholds := mapToThresholds(stringThresholds)
		floatThresholds = floatThresholds[:(min(uint32(len(floatThresholds)), bucketlimit))]
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
			lb = append(lb, gostatsd.HistogramThreshold(floatBucket))
		}
	}
	return lb
}

func findTag(a []string, prefix string) (string, bool) {
	for _, n := range a {
		if strings.HasPrefix(n, prefix) {
			return n, true
		}
	}
	return "", false
}

func min(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}
