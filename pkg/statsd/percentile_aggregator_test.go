package statsd

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/atlassian/gostatsd"
)

func Test_searchWhichBucket(t *testing.T) {
	tests := []struct {
		name    string
		buckets []int
		v       float64
		want    int
	}{
		{name: "foo1", buckets: []int{1, InfinityBucketSize}, v: 0, want: 1},
		{name: "foo2", buckets: []int{10, 20, 30, InfinityBucketSize}, v: 21, want: 30},
		{name: "foo3", buckets: []int{10, 20, 30, InfinityBucketSize}, v: 30, want: InfinityBucketSize},
		{name: "foo4", buckets: []int{InfinityBucketSize}, v: 21, want: InfinityBucketSize},
		{name: "foo5", buckets: []int{10, 20, 30, InfinityBucketSize}, v: 44, want: InfinityBucketSize},
		{name: "foo7", buckets: []int{10, 20, 30, InfinityBucketSize}, v: 5, want: 10},
		{name: "foo8", buckets: []int{10, 20, 30, InfinityBucketSize}, v: 11, want: 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := searchWhichBucket(tt.buckets, tt.v); got != tt.want {
				t.Errorf("searchWhichBucket() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Timer(values ...float64) gostatsd.Timer {
	return gostatsd.Timer{
		Values:  values,
		Tags:    []string{PercentileBucketsMarkerTag, BucketPrefix + PercentileBucketsPow4Algorithm},
		Buckets: map[int]int{},
	}
}

func TestAggregatePercentiles(t *testing.T) {
	//PercentileBucketsPow4Algorithm:          {4, 16, 64, 256, 1024, 4096, 16384},

	tests := []struct {
		name  string
		timer gostatsd.Timer
		want  map[int]int
	}{
		{
			name:  "foo",
			timer: Timer(1, 10, 11, 12, 500, 1000, 1023, 1024, 1025, 4000, 16384),
			want: map[int]int{
				// We include the zero-sized buckets in the comments here to make the test
				// more understandable.  We do not send save zero buckets in the map though.
				4:  1, // 1
				16: 3, // 10, 11, 12
				//64:    0,
				//256:   0,
				1024: 3, // 500, 1000, 1023
				4096: 3, // 1024, 1025, 4000
				//16384:  0,
				InfinityBucketSize: 1, // 16384
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buckets := AggregatePercentiles(tt.timer)
			assert.Equal(t, tt.want, buckets)
		})
	}
}
