package statsd

import (
	"math/bits"
	"strings"

	"github.com/atlassian/gostatsd"
)

const (
	InfinityBucketSize int = (1<<bits.UintSize)/2 - 1

	PercentileBucketsMarkerTag = "percentiles:true"

	BucketPrefix = "bucket:"

	PercentileBucketsPow2Algorithm          = "pow2"
	PercentileBucketsPow4Algorithm          = "pow4"
	PercentileBucketsLinearAlgorithm        = "linear"
	PercentileBucketsTailLatencyAlgorithm   = "tail-latency"
	PercentileBucketsRoundDoublingAlgorithm = "round-doubling"
)

// buckets must be sorted ascending
var bucketLimitMap = map[string][]int{
	PercentileBucketsPow2Algorithm:          {2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192},
	PercentileBucketsPow4Algorithm:          {4, 16, 64, 256, 1024, 4096, 16384},
	PercentileBucketsLinearAlgorithm:        {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000},
	PercentileBucketsTailLatencyAlgorithm:   {2, 8, 32, 64, 100, 200, 300, 400, 500, 750, 1000, 1250, 1500, 1750, 2000, 4000, 6000, 8000, 10000, 20000, 30000, 40000, 50000, 60000},
	PercentileBucketsRoundDoublingAlgorithm: {10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000},
}

func AggregatePercentiles(timer gostatsd.Timer) map[int]int {
	if count := len(timer.Values); count > 0 {
		if bucketAlgo := getBucketAlgo(timer.Tags); bucketAlgo != nil {
			if bucketSet, ok := bucketLimitMap[*bucketAlgo]; ok {
				return calculatePercentileBuckets(timer.Values, bucketSet)
			}
		}
	}
	return nil
}

func getBucketAlgo(tags []string) *string {
	if contains(tags, PercentileBucketsMarkerTag) {
		if match := findTag(tags, BucketPrefix); match != nil {
			algo := (*match)[len(BucketPrefix):]
			return &algo
		}
	}
	return nil
}

func searchWhichBucket(buckets []int, floatValue float64) int {
	intValue := int(floatValue)

	midpoint := len(buckets) / 2

	if buckets[midpoint] < intValue { // traverse RHS

		for i := midpoint + 1; i < len(buckets); i++ {
			if intValue < buckets[i] {
				return buckets[i]
			}
		}
		return buckets[len(buckets)+1] // value is max int

	} else if buckets[midpoint] == intValue {

		return buckets[midpoint+1]

	} else { // traverse LHS
		// i is the index wwe want the bucket to be at

		for i := midpoint; i > 0; i-- {
			if intValue >= buckets[i-1] {
				return buckets[i]
			}
		}
		return buckets[0] // value is less than lowest bucket limit

	}
}

func calculatePercentileBuckets(values []float64, buckets []int) map[int]int {
	// buckets are sorted ascending
	buckets = append(buckets, InfinityBucketSize)

	result := make(map[int]int)

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
func findTag(a []string, prefix string) *string {
	for _, n := range a {
		if strings.HasPrefix(n, prefix) {
			return &n
		}
	}
	return nil
}
