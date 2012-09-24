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
