package statsd

import (
	"math"
)

// round rounds a number to its nearest integer value
func round(v float64) float64 {
	return math.Floor(v + 0.5)
}

// average computes the average (mean) of a list of numbers
func average(vals []float64) float64 {
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

// thresholdStats calculates the mean and upper values of a list of values after a applying a minimum threshold
func thresholdStats(vals []float64, threshold float64) (mean, upper float64) {
	if count := len(vals); count > 1 {
		idx := int(round(((100 - threshold) / 100) * float64(count)))
		thresholdCount := count - idx
		thresholdValues := vals[:thresholdCount]

		mean = average(thresholdValues)
		upper = thresholdValues[len(thresholdValues)-1]
	} else {
		mean = vals[0]
		upper = vals[0]
	}
	return mean, upper
}
