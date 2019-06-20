package statsd

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/atlassian/gostatsd"
)

func timer(histogramThresholds string, values ...float64) gostatsd.Timer {
	return gostatsd.Timer{
		Values:    values,
		Tags:      []string{HistogramThresholdsTagPrefix + histogramThresholds},
		Histogram: map[gostatsd.HistogramThreshold]int{},
	}
}

func TestLatencyHistogram(t *testing.T) {
	tests := []struct {
		name  string
		timer gostatsd.Timer
		want  map[gostatsd.HistogramThreshold]int
	}{
		{
			name:  "happy path",
			timer: timer("10_30_45", 1, 10, 11, 12, 29, 30, 31, 45, 100, 100000),
			want: map[gostatsd.HistogramThreshold]int{
				10:                                       2,
				30:                                       6,
				45:                                       8,
				gostatsd.HistogramThreshold(math.Inf(1)): 10,
			},
		},
		{
			name:  "float thresholds",
			timer: timer("1.5_4_7.0", 1.4999, 1.5, 1.51, 4.0, 7.01),
			want: map[gostatsd.HistogramThreshold]int{
				1.5:                                      2,
				4:                                        4,
				7.0:                                      4,
				gostatsd.HistogramThreshold(math.Inf(1)): 5,
			},
		},
		{
			name:  "no timer values",
			timer: timer("1_5_10"),
			want: map[gostatsd.HistogramThreshold]int{
				1:                                        0,
				5:                                        0,
				10:                                       0,
				gostatsd.HistogramThreshold(math.Inf(1)): 0,
			},
		},
		{
			name:  "one non parsable thresholds",
			timer: timer("1_incorrect_10", 0, 10, 20),
			want: map[gostatsd.HistogramThreshold]int{
				1:                                        1,
				10:                                       2,
				gostatsd.HistogramThreshold(math.Inf(1)): 3,
			},
		},
		{
			name:  "totally unparsable tag",
			timer: timer("nothresoldsatall", 0, 10, 20),
			want: map[gostatsd.HistogramThreshold]int{
				gostatsd.HistogramThreshold(math.Inf(1)): 3,
			},
		},
		{
			name: "timer without the tag at all",
			timer: gostatsd.Timer{
				Values:    []float64{1, 2, 3},
				Tags:      []string{"some_different_tag:yep"},
				Histogram: map[gostatsd.HistogramThreshold]int{},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buckets := latencyHistogram(tt.timer)
			assert.Equal(t, tt.want, buckets)
		})
	}
}
