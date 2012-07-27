package statsd

import (
	"math"
)

func round(v float64) float64 {
	return math.Floor(v + 0.5)
}

func average(vals []float64) float64 {
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}
