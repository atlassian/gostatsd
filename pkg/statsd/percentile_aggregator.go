package statsd

import (
	"fmt"
	"strings"

	"golang.org/x/tools/container/intsets"

	"github.com/atlassian/gostatsd"
)

const (
	PosInfinityBucketLimit = intsets.MaxInt
	NegInfinityBucketLimit = intsets.MinInt

	PercentileBucketsMarkerTag = "percentiles:true"

	BucketPrefix = "bucket:"

	PercentileBucketsPow2Algorithm          = "pow2"
	PercentileBucketsPow4Algorithm          = "pow4"
	PercentileBucketsLinearAlgorithm        = "linear"
	PercentileBucketsTailLatencyAlgorithm   = "tail-latency"
	PercentileBucketsRoundDoublingAlgorithm = "round-doubling"
)

func makeBounds(uppers ...int) []gostatsd.BucketBounds {
	var arr []gostatsd.BucketBounds
	prev := NegInfinityBucketLimit
	for _, next := range uppers {
		if next <= prev {
			panic(fmt.Sprintf("Bad bucket bounds, not monotonically increasing. %v <= %v", next, prev))
		}
		arr = append(arr, gostatsd.BucketBounds{prev, next})
		prev = next
	}
	arr = append(arr, gostatsd.BucketBounds{prev, PosInfinityBucketLimit})
	return arr
}

// buckets must be sorted ascending
var bucketLimitMap = map[string][]gostatsd.BucketBounds{
	PercentileBucketsPow2Algorithm:          makeBounds(0, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192),
	PercentileBucketsPow4Algorithm:          makeBounds(0, 4, 16, 64, 256, 1024, 4096, 16384),
	PercentileBucketsLinearAlgorithm:        makeBounds(0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000),
	PercentileBucketsTailLatencyAlgorithm:   makeBounds(0, 2, 8, 32, 64, 100, 200, 300, 400, 500, 750, 1000, 1250, 1500, 1750, 2000, 4000, 6000, 8000, 10000, 20000, 30000, 40000, 50000, 60000),
	PercentileBucketsRoundDoublingAlgorithm: makeBounds(0, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000),
}

func AggregatePercentiles(timer gostatsd.Timer) map[gostatsd.BucketBounds]int {
	if count := len(timer.Values); count > 0 {
		if bucketAlgo := getBucketAlgo(timer.Tags); bucketAlgo != "" {
			if bucketSet, ok := bucketLimitMap[bucketAlgo]; ok {
				return calculatePercentileBuckets(timer.Values, bucketSet)
			}
		}
	}
	return nil
}

func getBucketAlgo(tags []string) string {
	if contains(tags, PercentileBucketsMarkerTag) {
		if match := findTag(tags, BucketPrefix); match != "" {
			return match[len(BucketPrefix):]
		}
	}
	return ""
}

func searchWhichBucket(buckets []gostatsd.BucketBounds, floatValue float64) gostatsd.BucketBounds {
	intValue := int(floatValue)

	midpoint := len(buckets) / 2

	if buckets[midpoint].Max < intValue { // traverse RHS

		for i := midpoint + 1; i < len(buckets); i++ {
			if intValue < buckets[i].Max {
				return buckets[i]
			}
		}
		return buckets[len(buckets)+1] // value is max int

	} else if buckets[midpoint].Max == intValue {

		return buckets[midpoint+1]

	} else { // traverse LHS
		// i is the index wwe want the bucket to be at

		for i := midpoint; i > 0; i-- {
			if intValue >= buckets[i-1].Max {
				return buckets[i]
			}
		}
		return buckets[0] // value is less than lowest bucket limit

	}
}

func calculatePercentileBuckets(values []float64, buckets []gostatsd.BucketBounds) map[gostatsd.BucketBounds]int {
	// buckets are sorted ascending
	result := make(map[gostatsd.BucketBounds]int)

	for _, floatValue := range values {
		targetBucket := searchWhichBucket(buckets, floatValue)
		result[targetBucket] += 1
	}
	return result
}

// Contains tells whether a contains x.
func contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

// returns the first string in a that begins with prefix
func findTag(a []string, prefix string) string {
	for _, n := range a {
		if strings.HasPrefix(n, prefix) {
			return n
		}
	}
	return ""
}
